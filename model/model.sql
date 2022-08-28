UPDATE df_1
set rating_average="0"
WHERE rating_average ISNULL;

CREATE TABLE tipos_show
(id INTEGER,
tipos TEXT);

INSERT INTO tipos_show
SELECT row_number() over(ORDER by type) as id,  type from df_2
GROUP by type;

CREATE TABLE paises
(id INTEGER,
pais TEXT);

INSERT INTO paises
SELECT row_number() over(ORDER by webChannel_country_name) as id,  webChannel_country_name from df_3
GROUP by webChannel_country_name;

CREATE TABLE transmision
(id INTEGER,
id_transmi INTEGER);

INSERT INTO transmision
SELECT row_number() over(ORDER by id) as id,  id from df_1
GROUP by id;