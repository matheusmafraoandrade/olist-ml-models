-- Databricks notebook source
WITH tb_join AS (

  SELECT DISTINCT
         t2.idVendedor,
         t3.*

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido AS t2
  ON t1.idPedido = t2.idPedido

  LEFT JOIN silver.olist.produto AS t3
  ON t2.idProduto = t3.idProduto

  WHERE t1.dtPedido > '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t2.idVendedor IS NOT NULL
),

tb_summary AS (

  SELECT idVendedor,
         avg(COALESCE(nrFotos,0)) AS avgFotos,
         avg(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS avgVolumeProduto,
         percentile(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) AS medianVolumeProduto,
         min(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS minVolumeProduto,
         max(vlComprimentoCm * vlAlturaCm * vlLarguraCm) AS maxVolumeProduto,
         
         count(DISTINCT CASE WHEN descCategoria = 'cama_mesa_banho' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriacama_mesa_banho,
         count(DISTINCT CASE WHEN descCategoria = 'beleza_saude' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriabeleza_saude,
         count(DISTINCT CASE WHEN descCategoria = 'esporte_lazer' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriaesporte_lazer,
         count(DISTINCT CASE WHEN descCategoria = 'informatica_acessorios' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriainformatica_acessorios,
         count(DISTINCT CASE WHEN descCategoria = 'moveis_decoracao' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriamoveis_decoracao,
         count(DISTINCT CASE WHEN descCategoria = 'utilidades_domesticas' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriautilidades_domesticas,
         count(DISTINCT CASE WHEN descCategoria = 'relogios_presentes' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriarelogios_presentes,
         count(DISTINCT CASE WHEN descCategoria = 'telefonia' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriatelefonia,
         count(DISTINCT CASE WHEN descCategoria = 'automotivo' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriaautomotivo,
         count(DISTINCT CASE WHEN descCategoria = 'brinquedos' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriabrinquedos,
         count(DISTINCT CASE WHEN descCategoria = 'cool_stuff' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriacool_stuff,
         count(DISTINCT CASE WHEN descCategoria = 'ferramentas_jardim' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriaferramentas_jardim,
         count(DISTINCT CASE WHEN descCategoria = 'perfumaria' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriaperfumaria,
         count(DISTINCT CASE WHEN descCategoria = 'bebes' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriabebes,
         count(DISTINCT CASE WHEN descCategoria = 'eletronicos' THEN idProduto END) / count(DISTINCT idProduto) AS pctCategoriaeletronicos

  FROM tb_join

  GROUP BY idVendedor
)

SELECT '2018-01-01' AS dtReference,
       *

FROM tb_summary
