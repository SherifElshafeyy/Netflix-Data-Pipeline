CREATE TABLE SRC_NETFLIX_DATA (
    URL NVARCHAR2(200),
    TITLE VARCHAR2(200),
    TYPE VARCHAR2(100),
    GENRES VARCHAR2(80),
    RELEASE_YEAR NUMBER(8,2),  
    IMDB_ID VARCHAR2(30),
    IMDB_AVERAGE_RATING NUMBER(10,2),  
    IMDB_NUM_VOTES NUMBER)15,2),
    AVAILABLE_COUNTRIES VARCHAR2(500)
);


CREATE TABLE TGT_NETFLIX_CLEANED_DATA (
    URL NVARCHAR2(200),
    TITLE VARCHAR2(250),
    TYPE VARCHAR2(80),
    GENRES VARCHAR2(80),
    RELEASE_YEAR NUMBER(38,0),
    IMDB_ID VARCHAR2(30),
    IMDB_AVERAGE_RATING FLOAT(126),
    IMDB_NUM_VOTES NUMBER)38,0),
    AVAILABLE_COUNTRIES VARCHAR2(500)
);



select * from tgt_netflix_cleaned_data

SELECT COUNT(*) as count_src_netflix FROM src_netflix_data 

select count(*) as count_tgt_netflix from tgt_netflix_cleaned_data

select * from tgt_netflix_cleaned_data where title = 'Interstellar' and genres = 'Science Fiction'

select * from src_netflix_data where title = 'Interstellar' and genres = 'Science Fiction'

UPDATE src_netflix_data 
SET release_year = 2014, 
    imdb_average_rating = 9.8 
WHERE imdb_id = 'ab123456';

commit;