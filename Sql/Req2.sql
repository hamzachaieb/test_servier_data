SELECT

  client_id,

  SUM(CASE WHEN product_type = 'MEUBLE' THEN prod_qty ELSE 0 END) AS ventes_meuble,

  SUM(CASE WHEN product_type = 'DECO' THEN prod_qty ELSE 0 END) AS ventes_deco

FROM `some_project.servier. TRANSACTION  ` AS transactions

LEFT JOIN `some_project.servier. PRODUCT_NOMENCLATURE ` AS product_nomenclature ON transactions.prod_id = product_nomenclature.product_id

WHERE transactions.date >= DATE(2019, 1, 1) AND transactions.date <= DATE(2019, 1, 31)

GROUP BY client_id
