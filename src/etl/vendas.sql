WITH tb_pedido_item AS (

  SELECT t2.*,
         t1.dtPedido

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '{date}'
  AND t1.dtPedido >= add_months('{date}', -6)
  AND t2.idVendedor IS NOT NULL
),

tb_summary AS (

  SELECT idVendedor,
         count(DISTINCT idPedido) AS qtdPedidos,
         count(DISTINCT date(dtPedido)) AS qtdDias,
         count(idProduto) AS qtdItens,
         datediff('{date}', max(dtPedido)) AS qtdRecencia,
         sum(vlPreco) / count(distinct idPedido) AS avgTicket,
         avg(vlPreco) AS avgValorProduto,
         max(vlPreco) AS maxValorProduto,
         min(vlPreco) AS minValorProduto,
         count(idProduto) / count(DISTINCT idPedido) AS avgProdutoPedido

  FROM tb_pedido_item
  
  GROUP BY idVendedor
),

tb_pedido_summary AS (

  SELECT idVendedor,
         idPedido,
         sum(vlPreco) AS vlPreco

  FROM tb_pedido_item

  GROUP BY idVendedor, idPedido
),

tb_min_max AS (  

  SELECT idVendedor,
         min(vlPreco) AS minVlPedido,
         max(vlPreco) AS maxVlPedido

  FROM tb_pedido_summary

  GROUP BY idVendedor
),

tb_life AS (
  -- Aqui não pegamos os últimos 6 meses, pois queremos o histórico completo do vendedor
  SELECT t2.idVendedor,
         sum(vlPreco) AS LTV,
         max(datediff('{date}', dtPedido)) AS qtdeDiasBase

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '{date}'
  AND t2.idVendedor IS NOT NULL

  GROUP BY t2.idVendedor
),

tb_dtpedido AS (

  SELECT DISTINCT idVendedor,
         date(dtPedido) AS dtPedido

  FROM tb_pedido_item

  ORDER BY 1,2
),

tb_lag AS (

  SELECT *,
         lag(dtPedido) OVER (PARTITION BY idVendedor ORDER BY dtPedido) as lag1

  FROM tb_dtpedido
),

tb_intervalo AS (

  SELECT idVendedor,
         avg(datediff(dtPedido, lag1)) AS avgIntervaloVendas

  FROM tb_lag

  GROUP BY idVendedor
)

SELECT '{date}' AS dtReference,
       NOW() as dtIngestion,
       t1.*,
       t2.minVlPedido,
       t2.maxVlPedido,
       t3.LTV,
       t3.qtdeDiasBase,
       t4.avgIntervaloVendas

FROM tb_summary AS t1

LEFT JOIN tb_min_max AS t2
ON t1.idVendedor = t2.idVendedor

LEFT JOIN tb_life AS t3
ON t1.idVendedor = t3.idVendedor

LEFT JOIN tb_intervalo AS t4
ON t1.idVendedor = t4.idVendedor