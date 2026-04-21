ALTER TABLE cfa_sem_global_clusters
  DROP CONSTRAINT IF EXISTS cfa_sem_global_clusters_semantic_subdir_week_chk;

ALTER TABLE cfa_sem_review_queue
  DROP CONSTRAINT IF EXISTS cfa_sem_review_queue_semantic_subdir_week_chk;

DELETE FROM cfa_sem_global_clusters
  WHERE EXTRACT(DOW FROM to_date(semantic_subdir, 'YYYY-MM-DD')) != 1;

DELETE FROM cfa_sem_review_queue
  WHERE EXTRACT(DOW FROM to_date(semantic_subdir, 'YYYY-MM-DD')) != 1;

ALTER TABLE cfa_sem_global_clusters
  ADD CONSTRAINT cfa_sem_global_clusters_semantic_subdir_week_chk
  CHECK (
    semantic_subdir ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    AND to_char(to_date(semantic_subdir, 'YYYY-MM-DD'), 'YYYY-MM-DD') = semantic_subdir
    AND EXTRACT(DOW FROM to_date(semantic_subdir, 'YYYY-MM-DD')) = 1
  );

ALTER TABLE cfa_sem_review_queue
  ADD CONSTRAINT cfa_sem_review_queue_semantic_subdir_week_chk
  CHECK (
    semantic_subdir ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
    AND to_char(to_date(semantic_subdir, 'YYYY-MM-DD'), 'YYYY-MM-DD') = semantic_subdir
    AND EXTRACT(DOW FROM to_date(semantic_subdir, 'YYYY-MM-DD')) = 1
  );
