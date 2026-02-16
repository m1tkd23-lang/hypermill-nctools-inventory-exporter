# src/hypermill_nctools_inventory_exporter/queries.py

NCTOOLS_FOR_FOLDER_SQL_TEMPLATE = r"""
SELECT
  ? AS nctools_folder_path,
  nt.nc_number_val AS nc_number,
  nt.nc_name       AS nc_name,
  nt.comment       AS nc_comment,
  t.name           AS tool_name,
  h.name           AS holder_name,
  (
    SELECT GROUP_CONCAT(x.ext, ' / ')
    FROM (
      SELECT printf('%d:%s(%.3f)', c.position, e.name, c.{reach_col}) AS ext
      FROM Components c
      JOIN Extensions e ON e.extension_id = c.extension_id
      WHERE c.nctool_id = nt.id
      ORDER BY c.position
    ) x
  ) AS extensions,
  nt.gage_length   AS gage_length,
  nt.tool_length   AS tool_length,
  COALESCE((
    SELECT SUM(c.{reach_col})
    FROM Components c
    WHERE c.nctool_id = nt.id
  ), 0) AS ext_reach_sum,
  (nt.tool_length + COALESCE((
    SELECT SUM(c.{reach_col}) FROM Components c WHERE c.nctool_id = nt.id
  ), 0)) AS overhang_est,
  nt.id            AS nctool_id
FROM NCTools nt
LEFT JOIN Tools   t ON t.id = nt.tool_id
LEFT JOIN Holders h ON h.id = nt.holder_id
WHERE nt.folder_id = ?
ORDER BY nt.nc_number_val
"""

NCTOOLS_ALL_FAST_SQL_TEMPLATE = r"""
WITH
root AS (
  SELECT folder_id AS root_id
  FROM Folders
  WHERE name = 'NCTools'
  LIMIT 1
),
folder_tree(folder_id, parent_id, name, path) AS (
  SELECT f.folder_id, f.parent_id, f.name, f.name AS path
  FROM Folders f, root
  WHERE f.parent_id = root.root_id

  UNION ALL

  SELECT c.folder_id, c.parent_id, c.name, folder_tree.path || '\' || c.name
  FROM Folders c
  JOIN folder_tree ON c.parent_id = folder_tree.folder_id
),
comp_sorted AS (
  SELECT
    c.nctool_id,
    c.position,
    e.name AS ext_name,
    c.{reach_col} AS reach_val
  FROM Components c
  JOIN Extensions e ON e.extension_id = c.extension_id
),
comp_agg AS (
  SELECT
    nctool_id,
    SUM(reach_val) AS ext_reach_sum,
    GROUP_CONCAT(ext_str, ' / ') AS extensions
  FROM (
    SELECT
      nctool_id,
      position,
      reach_val,  -- ★ これを追加（外側SUM用）
      printf('%d:%s(%.3f)', position, ext_name, reach_val) AS ext_str
    FROM comp_sorted
    ORDER BY nctool_id, position
  )
  GROUP BY nctool_id
)
SELECT
  ft.path           AS nctools_folder_path,
  nt.nc_number_val  AS nc_number,
  nt.nc_name        AS nc_name,
  nt.comment        AS nc_comment,
  t.name            AS tool_name,
  h.name            AS holder_name,
  ca.extensions     AS extensions,
  nt.gage_length    AS gage_length,
  nt.tool_length    AS tool_length,
  COALESCE(ca.ext_reach_sum, 0) AS ext_reach_sum,
  (nt.tool_length + COALESCE(ca.ext_reach_sum, 0)) AS overhang_est,
  nt.id             AS nctool_id
FROM NCTools nt
JOIN folder_tree ft ON ft.folder_id = nt.folder_id
LEFT JOIN Tools   t ON t.id = nt.tool_id
LEFT JOIN Holders h ON h.id = nt.holder_id
LEFT JOIN comp_agg ca ON ca.nctool_id = nt.id
ORDER BY ft.path, nt.nc_number_val
"""
