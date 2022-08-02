--
-- PostgreSQL database dump
--

-- Dumped from database version 14.4 (Debian 14.4-1.pgdg110+1)
-- Dumped by pg_dump version 14.4 (Debian 14.4-1.pgdg110+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: track_applied_migration(); Type: FUNCTION; Schema: public; Owner: postgres
--

CREATE FUNCTION public.track_applied_migration() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE _current_version integer;
BEGIN
    SELECT COALESCE(MAX(version),0) FROM schema_migrations_history INTO _current_version;
    IF new.dirty = 'f' AND new.version > _current_version THEN
        INSERT INTO schema_migrations_history(version) VALUES (new.version);
    END IF;
    RETURN NEW;
END;
$$;


ALTER FUNCTION public.track_applied_migration() OWNER TO postgres;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.schema_migrations (
    version bigint NOT NULL,
    dirty boolean NOT NULL
);


ALTER TABLE public.schema_migrations OWNER TO postgres;

--
-- Name: schema_migrations_history; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.schema_migrations_history (
    id integer NOT NULL,
    version bigint NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.schema_migrations_history OWNER TO postgres;

--
-- Name: schema_migrations_history_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

CREATE SEQUENCE public.schema_migrations_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER TABLE public.schema_migrations_history_id_seq OWNER TO postgres;

--
-- Name: schema_migrations_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: postgres
--

ALTER SEQUENCE public.schema_migrations_history_id_seq OWNED BY public.schema_migrations_history.id;


--
-- Name: todos; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.todos (
    id integer NOT NULL,
    title text NOT NULL,
    body text NOT NULL,
    created_by integer
);


ALTER TABLE public.todos OWNER TO postgres;

--
-- Name: todos_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.todos ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.todos_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.users (
    id integer NOT NULL,
    name text NOT NULL
);


ALTER TABLE public.users OWNER TO postgres;

--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: postgres
--

ALTER TABLE public.users ALTER COLUMN id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.users_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: schema_migrations_history id; Type: DEFAULT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.schema_migrations_history ALTER COLUMN id SET DEFAULT nextval('public.schema_migrations_history_id_seq'::regclass);


--
-- Name: schema_migrations_history schema_migrations_history_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.schema_migrations_history
    ADD CONSTRAINT schema_migrations_history_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: todos todos_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.todos
    ADD CONSTRAINT todos_pkey PRIMARY KEY (id);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations track_applied_migrations; Type: TRIGGER; Schema: public; Owner: postgres
--

CREATE TRIGGER track_applied_migrations AFTER INSERT ON public.schema_migrations FOR EACH ROW EXECUTE FUNCTION public.track_applied_migration();


--
-- Name: todos todos_created_by_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.todos
    ADD CONSTRAINT todos_created_by_fkey FOREIGN KEY (created_by) REFERENCES public.users(id);


--
-- PostgreSQL database dump complete
--

