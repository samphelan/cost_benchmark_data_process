DROP TABLE IF EXISTS quotes_project.vbrp_top_item;
CREATE TABLE IF NOT EXISTS quotes_project.vbrp_top_item (material_matnr STRING, created_on_erdat STRING, material_group_2_mvgr2 STRING);
INSERT INTO TABLE quotes_project.vbrp_top_item SELECT material_matnr,created_on_erdat,material_group_2_mvgr2 FROM saperp.vbrp;