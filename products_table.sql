CREATE TABLE products (
  id integer NOT NULL,
  created timestamp with time zone NOT NULL,
  modified timestamp with time zone NOT NULL,
  description character varying(255) NOT NULL,
  amount numeric(12,2) NOT NULL
);
