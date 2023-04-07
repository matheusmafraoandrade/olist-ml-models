-- Databricks notebook source
SELECT DISTINCT
      t1.idPedido,
      t2.idVendedor
      
FROM silver.olist.pedido AS t1

LEFT JOIN silver.olist.item_pedido as t2
ON t1.idPedido = t2.idPedido

WHERE dtPedido < '2018-01-01'
AND dtPedido >= add_months('2018-01-01', -6)

-- COMMAND ----------

WITH tb_pedidos AS (
  
  -- Trazer informações do vendedor para a tabela de pedidos (só existe na item_pedido). Distinct garante que tenha apenas um vendedor por linha
  SELECT DISTINCT
        t1.idPedido,
        t2.idVendedor

  FROM silver.olist.pedido AS t1

  LEFT JOIN silver.olist.item_pedido as t2
  ON t1.idPedido = t2.idPedido

  WHERE t1.dtPedido < '2018-01-01'
  AND t1.dtPedido >= add_months('2018-01-01', -6)
  AND t2.idVendedor IS NOT NULL

),

tb_join AS (

  -- Trazer informações sobre meios de pagamento
  SELECT t1.idVendedor,
         t2.*

  FROM tb_pedidos AS t1

  LEFT JOIN silver.olist.pagamento_pedido AS t2
  ON t1.idPedido = t2.idPedido

),

tb_group AS (

  -- Agrupar por vendedor e tipo de pagamento (uma linha para cada vendedor e tipo de pagamento. EX: v1 | cdt, v1 | dbt, ...)
  SELECT idVendedor,
         descTipoPagamento,
         count(distinct idPedido) AS qtdePedidoMeioPagamento,
         sum(vlPagamento) AS vlPedidoMeioPagamento

  FROM tb_join

  GROUP BY idVendedor, descTipoPagamento
  ORDER BY idVendedor, descTipoPagamento

)

SELECT idVendedor,

      -- QUANTIDADE DE PEDIDOS POR MEIO DE PAGAMENTO
      sum(CASE WHEN descTipoPagamento = 'boleto' 
          then qtdePedidoMeioPagamento else 0 end) as qtde_boleto_pedido,
      sum(CASE WHEN descTipoPagamento = 'credit_card' 
          then qtdePedidoMeioPagamento else 0 end) as qtde_credit_card_pedido,
      sum(CASE WHEN descTipoPagamento = 'voucher' 
          then qtdePedidoMeioPagamento else 0 end) as qtde_voucher_pedido,
      sum(CASE WHEN descTipoPagamento = 'debit_card' 
          then qtdePedidoMeioPagamento else 0 end) as qtde_debit_card_pedido,

      -- VALOR TOTAL POR MEIO DE PAGAMENTO
      sum(CASE WHEN descTipoPagamento = 'boleto' 
          then vlPedidoMeioPagamento else 0 end) as valor_boleto_pedido,
      sum(CASE WHEN descTipoPagamento = 'credit_card' 
          then vlPedidoMeioPagamento else 0 end) as valor_credit_card_pedido,
      sum(CASE WHEN descTipoPagamento = 'voucher' 
          then vlPedidoMeioPagamento else 0 end) as valor_voucher_pedido,
      sum(CASE WHEN descTipoPagamento = 'debit_card' 
          then vlPedidoMeioPagamento else 0 end) as valor_debit_card_pedido,

      -- PERCENTUAL DA QUANTIDADE DE PEDIDOS POR MEIO DE PAGAMENTO
      sum(CASE WHEN descTipoPagamento = 'boleto' 
          then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento)
          as pct_qtd_boleto_pedido,
      sum(CASE WHEN descTipoPagamento = 'credit_card' 
          then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento)
          as pct_qtd_credit_card_pedido,
      sum(CASE WHEN descTipoPagamento = 'voucher' 
          then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento)
          as pct_qtd_voucher_pedido,
      sum(CASE WHEN descTipoPagamento = 'debit_card' 
          then qtdePedidoMeioPagamento else 0 end) / sum(qtdePedidoMeioPagamento)
          as pct_qtd_debit_card_pedido,

      -- PERCENTUAL DO VALOR TOTAL POR MEIO DE PAGAMENTO
      sum(CASE WHEN descTipoPagamento = 'boleto' 
          then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento)
          as pct_valor_boleto_pedido,
      sum(CASE WHEN descTipoPagamento = 'credit_card' 
          then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento)
          as pct_valor_credit_card_pedido,
      sum(CASE WHEN descTipoPagamento = 'voucher' 
          then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento)
          as pct_valor_voucher_pedido,
      sum(CASE WHEN descTipoPagamento = 'debit_card' 
          then vlPedidoMeioPagamento else 0 end) / sum(vlPedidoMeioPagamento)
          as pct_valor_debit_card_pedido

FROM tb_group

GROUP BY 1

-- COMMAND ----------


