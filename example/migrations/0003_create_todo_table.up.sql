BEGIN;

CREATE TABLE todos (
    id INT GENERATED ALWAYS AS IDENTITY,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    created_by INT REFERENCES users (id),
    PRIMARY KEY (id)
);

COMMIT;