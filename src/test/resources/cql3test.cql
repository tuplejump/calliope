DROP KEYSPACE cql3_test;

CREATE KEYSPACE cql3_test WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '3'
};

USE cql3_test;

CREATE TABLE emp_read_test (
  deptid int,
  empid int,
  first_name text,
  last_name text,
  PRIMARY KEY (deptid, empid)
);

CREATE TABLE emp_write_test (
  deptid int,
  empid int,
  first_name text,
  last_name text,
  PRIMARY KEY (deptid, empid)
);

CREATE INDEX firstNameIndex ON emp_read_test (first_name);

insert into emp_read_test(deptid, empid, first_name, last_name) values(20,105, 'jack', 'carpenter');
insert into emp_read_test(deptid, empid, first_name, last_name) values(20, 106, 'john',  'grumpy');
insert into emp_read_test(deptid, empid, first_name, last_name) values(21,   105, 'james', 'dawson');
insert into emp_read_test(deptid, empid, first_name, last_name) values(21, 108, 'alice', 'mathews');
insert into emp_read_test(deptid, empid, first_name, last_name) values(21, 110, 'estonio', 'perreze');


CREATE TABLE data_type_test (
  user_id uuid,
  name text,
  email ascii,
  age_in_years int,
  weight float,
  is_married boolean,
  ipaddress inet,
  profile varchar,
  places set<text>,
  props map<text, text>,
  degrees list<text>,
  salary decimal,
  some_blob blob,
  created_time timestamp,
  some_big_int varint,
  latitude double,
  longitude double,
  PRIMARY KEY(user_id)
);

CREATE INDEX dtest_email ON cql3_test.data_type_test (email);

CREATE INDEX dtest_name ON cql3_test.data_type_test (name);

CREATE INDEX dtest_weight ON cql3_test.data_type_test (weight);

INSERT INTO data_type_test(user_id, name, email, age_in_years, weight, is_married, ipaddress, profile, places, props,
  degrees, salary, some_blob, created_time, some_big_int, latitude, longitude) values(now(), 'jack', 'jack@gmail.com',
  28, 63.45, false, '198.100.34.62', 'jack fowler is brilliant analyst', {'new york', 'london'}, {'color' : 'red', 'car' : 'bmw'},
  ['btech', 'ms', 'phd', 'phd'], 2300.50, textAsBlob('some blobl is here'), '2014-01-01 08:31', 30000, 120.45, 70.98);

INSERT INTO data_type_test(user_id, name, email, age_in_years, weight, is_married, ipaddress, profile, places, props,
  degrees, salary, some_blob, created_time, some_big_int, latitude, longitude) values(now(), 'jill', 'jill@gmail.com',
  28, 63.45, false, '198.100.34.62', 'jill stewart is a keen data scientist', {'new york', 'london'}, {'color' : 'pink', 'car' : 'cooper'},
  ['btech', 'ms', 'phd', 'phd'], 2300.50, textAsBlob('some blobl is here'), '2014-01-01 08:31', 30000, 120.45, 70.98);

INSERT INTO data_type_test(user_id, name, email, age_in_years, weight, is_married, ipaddress, profile, places, props,
  degrees, salary, some_blob, created_time, some_big_int, latitude, longitude) values(now(), 'jack', 'jack@yahoo.com',
  28, 63.45, true, '198.100.34.62', 'jack graham is smart developer', {'new york', 'london'}, {'color' : 'red', 'car' : 'bmw'},
  ['btech', 'ms', 'phd', 'phd'], 2300.50, textAsBlob('some blob is here'), '2014-01-01 08:31', 30000, 120.45, 70.98);

INSERT INTO data_type_test(user_id, name, email, age_in_years, weight, is_married, ipaddress, profile, places, props,
  degrees, salary, some_blob, created_time, some_big_int, latitude, longitude) values(now(), 'jane', 'jane@gmail.com',
  28, 63.45, false, '198.100.34.62', 'jane trippiani is an amazing ux designer', {'new york', 'london'}, {'color' : 'red', 'car' : 'bmw'},
  ['btech', 'ms', 'phd', 'phd'], 2300.50, textAsBlob('some blobl is here'), '2014-01-01 08:31', 30000, 120.45, 70.98);

CREATE TABLE cql3_test.emp_score_test (
  org text,
  dept text,
  year int,
  emp text,
  app_score double,
  PRIMARY KEY((org), dept, year, emp)
);

CREATE INDEX emp_score_index ON cql3_test.emp_score_test (app_score);

INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'ict', 2001, 'johnny@tuplejump.com', 20.39);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'ict', 2001, 'jannet@tuplejump.com', 18.45);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'dev', 2001, 'rachael@tuplejump.com', 21.03);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'dev', 2001, 'lincoln@tuplejump.com', 16.78);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'dev', 2001, 'raymond@tuplejump.com', 19.06);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'support', 2001, 'eric@tuplejump.com', 22.87);

INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'ict', 2002, 'johnny@tuplejump.com', 22.34);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'ict', 2002, 'jannet@tuplejump.com', 19.32);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'dev', 2002, 'rachael@tuplejump.com', 22.00);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'dev', 2002, 'lincoln@tuplejump.com', 21.40);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'dev', 2002, 'raymond@tuplejump.com', 23.21);
INSERT INTO cql3_test.emp_score_test (org, dept, year, emp, app_score)  VALUES('tuplejump', 'support', 2002, 'eric@tuplejump.com', 24.10);


CREATE TABLE counter_test (
  user text,
  login_count counter,
  PRIMARY KEY(user));
