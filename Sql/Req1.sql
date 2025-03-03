SELECT date, SUM(prod_qty) AS ventes

FROM `some_project.servier.TRANSACTION`

WHERE date >= DATE(2019, 1, 1) AND date <= DATE(2019, 1, 31)

GROUP BY date