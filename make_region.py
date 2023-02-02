# -*- coding:utf-8 -*-
import json
import os
import codecs
from collections import OrderedDict

ddl = """

CREATE TABLE `region` (
  `id` varchar(10) NOT NULL COMMENT '地区主键编号',
  `name` varchar(50) NOT NULL COMMENT '地区名称',
  `parent_id` varchar(10) DEFAULT NULL COMMENT '地区父id',
  `level` int(2) DEFAULT NULL COMMENT ' 1-省、自治区、直辖市 2-地级市, 3-市辖区、县',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='地区表';

"""

sql_tmp =  "INSERT IGNORE INTO region (`id`, `name`, `parent_id`, `level`) VALUES ('%s', '%s', '%s', '%s');\n"
region_path = "mysql-region"

def make_data():
    #只读取省市县数据。
    source_file_list = ['county', 'city',  'province' ]
    for k in list(reversed(source_file_list)):
        data = codecs.open('json/%s.json' % k, 'r', 'utf-8').read()
        json_data = json.loads(data)
        mysql_data_list = []

        if k == 'province':
            for index, province in enumerate(json_data):
                tmp_id = province['id']
                mysql_data = sql_tmp % (province['id'][0:6], province['name'], 0, 1)  # noqa
                mysql_data_list.append(mysql_data)

        if k == 'city':
            index = 0
            for province_id in sorted(json_data.keys()):
                for city in json_data[province_id]:
                    index += 1
                    mysql_data = sql_tmp % (city['id'][0:6], city['name'], province_id[0:6], 2)  # noqa
                    mysql_data_list.append(mysql_data)

        if k == 'county':
            index = 0
            for city_id in sorted(json_data.keys()):
                for county in json_data[city_id]:
                    index += 1
                    mysql_data = sql_tmp % (county['id'][0:6], county['name'], city_id[0:6], 3)  # noqa
                    mysql_data_list.append(mysql_data)

        if k in ['province', 'city', 'county']:
            out_mysql = codecs.open(region_path + '/%s.sql' % k, 'w', 'utf-8')
            index = 0
            start = 0
            while start < len(mysql_data_list):
                end = start + 1000
                out_mysql.write(''.join(mysql_data_list[start:end]))
                start = end

            out_mysql.close()


def main():

    if not os.path.exists(region_path):
    	os.mkdir(region_path)
    make_data()


if __name__ == '__main__':
    main()
