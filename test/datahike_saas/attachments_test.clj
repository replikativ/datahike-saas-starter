(ns datahike-saas.attachments-test
  "Blob attachments (:db.type/store-ref) over a temp FILE-backed datahike db — no object
   store needed, but a real persisted index so the GC mark (`reachable-store-refs`) runs.
   (A bare :memory db can't be marked: 'Index needs to be properly flushed before marking.')
   Covers the in-store round-trip and the mark used to sweep external blobs."
  (:require [clojure.test :refer [deftest is testing use-fixtures]]
            [clojure.java.io :as io]
            [datahike.api :as d]
            [datahike-saas.example.schema :as schema]
            [datahike-saas.example.domain :as dom]
            [datahike-saas.example.attachments :as att]))

(def ^:dynamic *conn* nil)

(defn- rm-rf [f]
  (when (.isDirectory f) (run! rm-rf (.listFiles f)))
  (io/delete-file f true))

(defn file-conn-fixture [f]
  (let [dir (io/file (System/getProperty "java.io.tmpdir")
                     (str "dh-saas-att-" (random-uuid)))
        cfg {:store {:backend :file :path (.getPath dir) :id (random-uuid)}
             :keep-history? false}]
    (d/create-database cfg)
    (let [conn (schema/ensure-schema! (d/connect cfg))]
      (try
        (binding [*conn* conn] (f))
        (finally
          (d/release conn)
          (d/delete-database cfg)
          (rm-rf dir))))))

(use-fixtures :each file-conn-fixture)

(deftest in-store-attach-roundtrip
  (let [c *conn*
        {:keys [id]} (dom/create-issue! c {:title "crash" :reporter "alice"})
        bytes (.getBytes "a screenshot's worth of bytes" "UTF-8")
        blob-id (att/attach! c id {:filename "shot.png" :content-type "image/png" :bytes bytes})]
    (testing "attach! stores the bytes and returns a content id"
      (is (uuid? blob-id))
      (is (= (seq bytes) (seq (att/fetch c blob-id))) "fetch round-trips the exact bytes"))
    (testing "content-addressed: the same bytes give the same id (dedup)"
      (is (= blob-id (att/attach! c id {:filename "again.png" :content-type "image/png" :bytes bytes}))))
    (testing "the attachment is on the issue, tagged :in-store"
      (let [a (first (:issue/attachments (dom/issue @c id)))]
        (is (= blob-id (:attachment/blob a)))
        (is (= :in-store (:attachment/storage a)))
        (is (= (count bytes) (:attachment/size a)))))
    (testing "the GC mark names the live blob"
      (is (contains? (att/live-blob-ids @c) blob-id)))))

(deftest external-ref-mark-and-sweep
  (let [c *conn*
        {:keys [id]} (dom/create-issue! c {:title "big upload" :reporter "alice"})
        ext-id (att/record-external! c id {:blob-id (random-uuid) :filename "big.pdf"
                                           :content-type "application/pdf" :size 1048576})]
    (testing "an external (S3-direct) ref is marked live but stores no bytes locally"
      (is (contains? (att/live-blob-ids @c) ext-id))
      (is (= :s3-direct (:attachment/storage (first (:issue/attachments (dom/issue @c id)))))))
    (testing "garbage-ids keeps only ids the database no longer names"
      (let [orphan (random-uuid)
            garbage (att/garbage-ids @c [ext-id orphan])]
        (is (= [orphan] garbage))))))
