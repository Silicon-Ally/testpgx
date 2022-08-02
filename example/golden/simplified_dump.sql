-- Human Readable Schema

-- This schema is heavily simplified to assist human comprehension it is
-- presented out of order and should not be used for anything other than
-- reference. It is auto-generated by the testpgx package.




CREATE TABLE schema_migrations_history (
	applied_at timestamp with time zone DEFAULT now() NOT NULL,
	id integer NOT NULL,
	version bigint NOT NULL);
ALTER TABLE ONLY schema_migrations_history ADD CONSTRAINT schema_migrations_history_pkey PRIMARY KEY (id);
ALTER SEQUENCE schema_migrations_history_id_seq OWNED BY schema_migrations_history.id;


CREATE TABLE todos (
	body text NOT NULL,
	created_by integer,
	id integer NOT NULL,
	title text NOT NULL);
ALTER TABLE todos ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME todos_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE ONLY todos ADD CONSTRAINT todos_pkey PRIMARY KEY (id);
ALTER TABLE ONLY todos ADD CONSTRAINT todos_created_by_fkey FOREIGN KEY (created_by) REFERENCES users(id);


CREATE TABLE users (
	id integer NOT NULL,
	name text NOT NULL);
ALTER TABLE users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME users_id_seq START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1);
ALTER TABLE ONLY users ADD CONSTRAINT users_pkey PRIMARY KEY (id);