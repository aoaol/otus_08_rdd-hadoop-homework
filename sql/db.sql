--DROP DATABASE IF EXISTS otus;
--CREATE DATABASE otus;

CREATE TABLE amount_by_distance (
    percent_rank           NUMERIC( 3, 2),
    total_trips            INT,
    min_distance           NUMERIC( 6, 2),
    median_distance        NUMERIC( 6, 2),
    median_duration        NUMERIC( 6, 2),
    median_total_amount    NUMERIC( 6, 2),
    median_tip_amount      NUMERIC( 6, 2),
    dur_total_amount       NUMERIC( 6, 2),
    dur_tip_amount         NUMERIC( 6, 2)
);
