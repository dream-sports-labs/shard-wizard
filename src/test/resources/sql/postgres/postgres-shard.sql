CREATE TABLE Orders
(
    order_id        VARCHAR(50) NOT NULL,
    order_name      VARCHAR(50) NOT NULL,
    order_date      VARCHAR(50) NOT NULL,
    order_amount    BIGINT      NOT NULL,
    user_id         VARCHAR(50) NOT NULL,
    PRIMARY KEY (order_id)
);
