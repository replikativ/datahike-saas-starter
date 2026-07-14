(ns datahike-saas.reader-demo
  "Direct-S3 reader demo + latency (Tier 3).

   A Tier-3 reader is stateless: it connects to the shared bucket and derefs. It
   auto-follows the writer with no streaming and no sync — but NOT for the reason
   you might guess, and the reason matters:

     `deref-conn` re-reads the branch head from the store **only when the writer
     backend is non-streaming** (datahike/src/datahike/connector.cljc). The default
     `:self` writer reports `:streaming? true`, so @conn on a :self connection just
     returns its in-memory atom. A Tier-3 reader follows the writer *because* it
     configures a `:datahike-server` (non-streaming) writer — which is also why a
     node left on the default :self writer would silently serve a FROZEN snapshot.

   ISOLATION (why this demo used to lie): datahike caches connections on
   [store-id, branch] — not on the writer backend. So a 'reader' pool opened in the
   same JVM as the writer gets handed the WRITER'S OWN connection, derefs its
   in-memory atom, and reports sub-millisecond reads with zero S3 traffic. The demo
   then 'passes' even if Tier 3 is entirely broken. We give the reader its own
   connection registry (`*connections*` is a dynamic var) so it is a genuine second
   node: a fresh connection, reading the shared bucket for real.

   Run: docker compose --profile tier1 up -d
        MINIO_PORT=9000 clj -M:bench -m datahike-saas.reader-demo
        MINIO_PORT=19000 ...   (behind bin/latency-proxy, to see it at cloud RTT)"
  (:require [datahike-saas.tenant :as tenant]
            [datahike-saas.domain :as dom]
            [datahike-saas.config :as config]
            [datahike-saas.harness :as h]
            [datahike.connections :as conns]
            [datahike.http.writer]))            ;; registers the :datahike-server writer

(def ^:const n-issues 20)
(def ^:const n-reads  200)

(defn- reader-base-cfg []
  (assoc (config/base-cfg)
         :writer {:backend :datahike-server              ;; non-streaming => deref re-reads the head
                  :url (or (System/getenv "SAAS_WRITER_URL") "http://localhost:8888")}))

(defn- measure! [label conn]
  (dotimes [_ 20] (dom/open-issues @conn))                ;; warm the node cache
  (let [hist (h/hist)]
    (dotimes [_ n-reads]
      (h/record! hist (second (h/time-ns #(dom/open-issues @conn)))))
    (let [s (h/summary hist)]
      (println (format "  %-30s p50 %6.2f ms   p99 %7.2f ms" label (:p50 s) (:p99 s)))
      s)))

(defn -main [& _]
  (let [slug  (str "rd-" (subs (str (random-uuid)) 0 8))
        wpool (tenant/create-pool)                        ;; WRITER node: :self writer, shared S3
        wc    (tenant/borrow wpool slug)]
    (dom/ensure-user! wc "alice" "Alice")
    (dom/create-issue! wc {:title "written by the writer" :reporter "alice"})
    (println "writer: created tenant" slug "+ 1 issue")

    ;; READER node: its own connection registry => a real, separate connection.
    (binding [conns/*connections* (atom {})]
      (let [rpool (tenant/create-pool {:base-cfg (reader-base-cfg)})
            rc    (tenant/borrow rpool slug)]

        ;; ── 1. it auto-follows the writer, with no coordination ───────────────
        (println "reader: sees (expect [1]):          " (mapv :issue/number (dom/open-issues @rc)))
        (dom/create-issue! wc {:title "second, after the reader connected" :reporter "alice"
                               :priority :issue.priority/urgent})
        (println "reader: after a writer commit ([2 1]):" (mapv :issue/number (dom/open-issues @rc)))
        (println "reader: sees the urgent issue:      " (mapv :issue/title (dom/by-priority @rc :issue.priority/urgent)))

        ;; ── 2. what the per-deref head GET costs ──────────────────────────────
        (dotimes [i n-issues]
          (dom/create-issue! wc {:title (str "issue " i) :reporter "alice"}))
        (println (format "\nread latency (%d reads, %d-issue tenant):" n-reads n-issues))
        (let [reader (measure! "reader (direct S3: head GET/deref)" rc)
              writer (measure! "writer (local :self conn)" wc)]
          (println (format "\n=> a Tier-3 read costs one head GET more than a local read: %+.2f ms at p50"
                           (- (:p50 reader) (:p50 writer))))
          (println "   (on a real bucket that GET is your store's RTT — which is what Tier 4 removes)"))

        (tenant/close-all! rpool)))

    (tenant/close-all! wpool)
    (println "\n=> direct-S3 readers auto-follow via deref — no streaming, no sync,")
    (println "   because their writer backend is NON-streaming. Keep :self and you freeze.")
    (shutdown-agents)))
