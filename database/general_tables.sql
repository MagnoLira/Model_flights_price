CREATE TABLE general.users (
    id uuid PRIMARY KEY NOT NULL,
    name character varying(255) NOT NULL,
    email character varying(255) NOT NULL,
    CONSTRAINT unique_email UNIQUE (email),
    password character varying(255) NOT NULL,
    email_verified_at timestamp without time zone,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);

CREATE TABLE general.solicitations (
    solicitation_id uuid PRIMARY KEY NOT NULL,
    user_id uuid REFERENCES general.users(id),
    status VARCHAR(255) NOT NULL,
    created_at timestamp without time zone NOT NULL
);
