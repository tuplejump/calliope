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




CREATE TABLE counter_test (
  user text,
  login_count counter
)
