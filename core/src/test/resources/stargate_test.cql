CREATE TABLE cql3_test.stargate_test (
  event_id timeuuid,
  post_pagename text,
  user_agent text,
  visit_referrer text,
  post_visid_high text,
  post_visid_low text,
  visit_num text,
  hit_time_gmt text,
  post_purchaseid text,
  post_product_list text,
  first_hit_referrer text,
  stargate text,
  PRIMARY KEY (event_id)
);

CREATE CUSTOM INDEX stargate_test_index1 ON cql3_test.stargate_test(stargate) USING 'com.tuplejump.stargate.RowIndex'
  WITH options = {
    'sg_options':'{
        "fields":{
            "post_pagename":{},
            "user_agent":{},
            "visit_referrer":{},
            "post_visid_high":{},
            "post_visid_low":{},
            "visit_num":{},
            "hit_time_gmt":{},
            "post_purchaseid":{},
            "post_product_list":{},
            "first_hit_referrer":{}
        }
    }'
  };

INSERT INTO cql3_test.stargate_test (event_id, post_pagename, user_agent, visit_referrer, post_visid_high,
 post_visid_low, visit_num, hit_time_gmt, post_purchaseid, post_product_list, first_hit_referrer) 
 values(now(), 'Page A', 'Chrome', 'http://facebook.com', '111', '111', '1', '1408007374', '', '', 'http://google.com');

INSERT INTO cql3_test.stargate_test (event_id, post_pagename, user_agent, visit_referrer, post_visid_high,
 post_visid_low, visit_num, hit_time_gmt, post_purchaseid, post_product_list, first_hit_referrer)
 values (now(), 'Page B', 'Chrome', 'http://facebook.com', '111', '111', '1', '1408007377', '', '', 'http://google.com');

INSERT INTO cql3_test.stargate_test (event_id, post_pagename, user_agent, visit_referrer, post_visid_high,
  post_visid_low, visit_num, hit_time_gmt, post_purchaseid, post_product_list, first_hit_referrer)
  values (now(), 'Page C', 'Chrome', 'http://facebook.com', '111', '111', '1', '1408007380', 'purchase1', ';ProductID1;1;40;,;ProductID2;1;20;', 'http://google.com');

INSERT INTO cql3_test.stargate_test (event_id, post_pagename, user_agent, visit_referrer, post_visid_high,
 post_visid_low, visit_num, hit_time_gmt, post_purchaseid, post_product_list, first_hit_referrer)
 values (now(),'Page B', 'Chrome', 'http://google.com', '222', '222', '1', '1408007379', '', '', 'http://google.com');

INSERT INTO cql3_test.stargate_test (event_id, post_pagename, user_agent, visit_referrer, post_visid_high,
 post_visid_low, visit_num, hit_time_gmt, post_purchaseid, post_product_list, first_hit_referrer)
 values (now(), 'Page C', 'Chrome', 'http://google.com', '222', '222', '1', '1408007381', '', '', 'http://google.com');

INSERT INTO cql3_test.stargate_test (event_id, post_pagename, user_agent, visit_referrer, post_visid_high,
 post_visid_low, visit_num, hit_time_gmt, post_purchaseid, post_product_list, first_hit_referrer)
 values (now(), 'Page A', 'Firefox', 'http://google.com', '222', '222', '1', '1408007382', '', '', 'http://google.com');

INSERT INTO cql3_test.stargate_test (event_id, post_pagename, user_agent, visit_referrer, post_visid_high,
 post_visid_low, visit_num, hit_time_gmt, post_purchaseid, post_product_list, first_hit_referrer)
 values (now(), 'Page A', 'Safari', 'http://google.com', '333', '333', '1', '1408007383', '', '', 'http://facebook.com');

INSERT INTO cql3_test.stargate_test (event_id, post_pagename, user_agent, visit_referrer, post_visid_high,
 post_visid_low, visit_num, hit_time_gmt, post_purchaseid, post_product_list, first_hit_referrer)
 values (now(), 'Page B', 'Safari', 'http://google.com', '333', '333', '1', '1408007386', '', '', 'http://facebook.com');