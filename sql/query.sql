DELETE FROM pending_order WHERE 1=1;
DELETE FROM order_history WHERE 1=1;
--SELECT clOrdID, status FROM order_history WHERE clOrdID > 0;
--drop table order_history;

-- SELECT clOrdID, status FROM order_history WHERE notify = 'FALSE'