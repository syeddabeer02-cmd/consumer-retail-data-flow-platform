-- Databricks SQL: select the target catalog and schema before executing.
-- Monetary totals must always remain separated by currency.
SELECT date_trunc('MONTH', to_date(event_date)) AS accounting_month,
       vendor_id, currency, status,
       count(*) AS records,
       sum(net_payable) AS expected_amount,
       sum(paid_amount) AS settled_amount,
       sum(variance) AS unresolved_variance
FROM current_reconciliation
GROUP BY date_trunc('MONTH', to_date(event_date)), vendor_id, currency, status
ORDER BY accounting_month, vendor_id, currency, status;
