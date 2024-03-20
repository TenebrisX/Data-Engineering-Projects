
DROP TABLE IF EXISTS public.shipping;



--shipping
CREATE TABLE public.shipping(
   ID                               serial,
   shipping_id                      BIGINT,
   sale_id                          BIGINT,
   order_id                         BIGINT,
   client_id                        BIGINT,
   payment_amount                   NUMERIC(14,2),
   state_datetime                   TIMESTAMP,
   product_id                       BIGINT,
   description                      text,
   vendor_id                        BIGINT,
   name_category                    text,
   base_country                     text,
   status                           text,
   state                            text,
   shipping_plan_datetime           TIMESTAMP,
   hours_to_plan_shipping           NUMERIC(14,2),
   shipping_transfer_description    text,
   shipping_transfer_rate           NUMERIC(14,3),
   shipping_country                 text,
   shipping_country_base_rate       NUMERIC(14,3),
   vendor_agreement_description     text,
   PRIMARY KEY (ID)
);
CREATE INDEX shipping_id ON public.shipping (shipping_id);
COMMENT ON COLUMN public.shipping.shipping_id is 'id of shipping of sale';