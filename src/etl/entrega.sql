-- Databricks notebook source
WITH tb_pedido AS (

  SELECT t1.idPedido,
         t2.idVendedor,
         t1.descSituacao,
         t1.dtPedido,
         t1.dtAprovado,
         t1.dtEntregue,
         t1.dtEstimativaEntrega,
         sum(vlFrete) AS totalFrete

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  WHERE dtpedido < '2018-01-01'
  AND dtPedido >= add_months('2018-01-01', -6)
  AND idVendedor IS NOT NULL

  GROUP BY t1.idPedido,
         t2.idVendedor,
         t1.descSituacao,
         t1.dtPedido,
         t1.dtAprovado,
         t1.dtEntregue,
         t1.dtEstimativaEntrega
)

SELECT '2018-01-01' as dtReference,
       idVendedor,
       count(DISTINCT CASE WHEN date(coalesce(dtEntregue, '2018-01-01')) > date(dtEstimativaEntrega) THEN idPedido END) /
         count(DISTINCT CASE WHEN descSituacao = 'delivered' THEN idPedido END) AS pctPedidoAtraso,
       count(CASE WHEN descSituacao = 'canceled' THEN idPedido END) / count(DISTINCT idPedido) AS pctPedidoCancelado,
       
       avg(totalFrete) as avgFrete,
       percentile(totalFrete, 0.5) as medianFrete,
       min(totalFrete) as minFrete,
       max(totalFrete) as maxFrete,
       
       avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtAprovado)) AS qtdDiasAprovadoEntrega,
       avg(datediff(coalesce(dtEntregue, '2018-01-01'), dtPedido)) AS qtdPedidoEntrega,
       
       avg(datediff(dtEstimativaEntrega, coalesce(dtEntregue, '2018-01-01'))) AS qtdeDiasEntregaPromessa
       
FROM tb_pedido

GROUP BY idVendedor
