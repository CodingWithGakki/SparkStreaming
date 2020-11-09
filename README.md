Spark微批处理流式开发实战

1.1 任务一：Python脚本生成测试数据

步骤 1编写Python脚本

进入/sparkTest/makedata/目录，使用vi命令编写Python脚本：autodatapython.py
代码如下：

    #coding:utf-8
    ###########################################
    
    # rowkey：随机的两位数 + 当前时间戳，并要确保该rowkey在表数据中唯一。
    
    # 列定义：行健，用户名，年龄，性别，商品ID，价格，门店ID，购物行为，电话，邮箱，购买日期
    
    #421564974572,Sgxrp,20,woman,152121,297.64,313015,scan,15516074528,JbiLDQmzwP@qq.com,2019-08-01
    #601564974572,Lbeuo,43,man,220902,533.13,313016,pv,15368753104,ezfrJSyuoR@163.com,2019-08-05###########################################
    import random
    import string
    import sys
    import time
    
    # 大小写字母
    
    alphabet_upper_list = string.ascii_uppercase
    alphabet_lower_list = string.ascii_lowercase
    
    # 随机生成指定位数的字符串
    
    def get_random(instr, length):
        # 从指定序列中随机获取指定长度的片段并组成数组，例如:['a', 't', 'f', 'v', 'y']
        res = random.sample(instr, length)
        # 将数组内的元素组成字符串
        result = ''.join(res)
        return result
    
    # 放置生成的并且不存在的rowkey
    
    rowkey_tmp_list = []
    
    # 制作rowkey
    
    def get_random_rowkey():
        import time
        pre_rowkey = ""
        while True:
            # 获取00~99的两位数字，包含00与99
            num = random.randint(00, 99)
            # 获取当前10位的时间戳
            timestamp = int(time.time())
            # str(num).zfill(2)为字符串不满足2位，自动将该字符串补0
            pre_rowkey = str(num).zfill(2) + str(timestamp)
            if pre_rowkey not in rowkey_tmp_list:
                rowkey_tmp_list.append(pre_rowkey)
                break
        return pre_rowkey
    
    # 创建用户名
    
    def get_random_name(length):
        name = string.capwords(get_random(alphabet_lower_list, length))
        return name
    
    # 获取年龄
    
    def get_random_age():
        return str(random.randint(18, 60))
    
    # 获取性别
    
    def get_random_sex():
        return random.choice(["woman", "man"])
    
    # 获取商品ID
    
    def get_random_goods_no():
        goods_no_list = ["220902", "430031", "550012", "650012", "532120","230121","250983", "480071", "580016", "950013", "152121","230121"]
    return random.choice(goods_no_list)
    
    # 获取商品价格（浮点型）
    
    def get_random_goods_price():
    	# 随机生成商品价格的整数位，1~999的三位数字，包含1与999
    	price_int = random.randint(1, 999)
    	# 随机生成商品价格的小数位，1~99的两位数字，包含1与99
    	price_decimal = random.randint(1, 99)
    	goods_price = str(price_int) +"." + str(price_decimal)
    	return goods_price
    
    # 获取门店ID
    
    def get_random_store_id():
    	store_id_list = ["313012", "313013", "313014", "313015", "313016","313017","313018", "313019", "313020", "313021", "313022","313023"]
        return random.choice(store_id_list)
    
    # 获取购物行为类型
    
    def get_random_goods_type():
        goods_type_list = ["pv", "buy", "cart", "fav","scan"]#点击、购买、加购、收藏、浏览
        return random.choice(goods_type_list)
    
    # 获取电话号码
    
    def get_random_tel():
        pre_list = ["130", "131", "132", "133", "134", "135", "136", "137", "138", "139", "147", "150",
                    "151", "152", "153", "155", "156", "157", "158", "159", "186", "187", "188"]
        return random.choice(pre_list) + ''.join(random.sample('0123456789', 8))
    
    # 获取邮箱名
    
    def get_random_email(length):
        alphabet_list = alphabet_lower_list + alphabet_upper_list
        email_list = ["163.com", "126.com", "qq.com", "gmail.com","huawei.com"]
        return get_random(alphabet_list, length) + "@" + random.choice(email_list)
    
    # 获取商品购买日期（统计最近7天数据）
    
    def get_random_buy_time():
        buy_time_list = ["2019-08-01", "2019-08-02", "2019-08-03", "2019-08-04", "2019-08-05", "2019-08-06", "2019-08-07"]
        return random.choice(buy_time_list)
    
    # 生成一条数据
    
    def get_random_record():
        return get_random_rowkey() + "," + get_random_name(
            5) + "," + get_random_age() + "," + get_random_sex() + "," + get_random_goods_no() + ","+get_random_goods_price()+ "," +get_random_store_id()+ "," +get_random_goods_type() + ","+ get_random_tel() + "," + get_random_email(
            10) + "," +get_random_buy_time()
    
    # 获取随机整数用于休眠
    
    def get_random_sleep_time():
        return random.randint(5, 10)
    
    # 将记录写到文本中
    
    def write_record_to_file():
    
        # 覆盖文件内容，重新写入
    
        f = open(sys.argv[1], 'w')
        i = 0
        while i < int(sys.argv[2]):
            record = get_random_record()
            f.write(record)
    
            # 换行写入
    
            f.write('\n')
            i += 1
        f.close()
    if __name__ == "__main__":
        write_record_to_file()

步骤 2创建目录

使用mkdir命令在/sparkTest下创建目录flume_spooldir，我们把Python脚本模拟生成的数据放到此目录下，后面Flume就监控这个文件下的目录，以读取数据。

1.2 任务二：配置Kafka

在kafka中创建topic cxhcip,3分区，2副本

1.3 任务三：配置Flume采集数据

监控指定目录，有新数据产生写入kafka中

1.4 任务四：MySQL中准备结果表与维度表数据

执行下面的SQL语句：

---

-- 创建商品信息纬度表

---

    DROP TABLE IF EXISTS `goods_info`;
    CREATE TABLE `goods_info` (
      `goods_no` varchar(30) NOT NULL,
      `goods_name` varchar(30) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;



---

-- 插入商品信息样例数据

---

    INSERT INTO `goods_info` VALUES ('220902', '杭州丝绸');
    INSERT INTO `goods_info` VALUES ('430031', '西湖龙井');
    INSERT INTO `goods_info` VALUES ('550012', '西湖莼菜');
    INSERT INTO `goods_info` VALUES ('650012', '张小泉剪刀');
    INSERT INTO `goods_info` VALUES ('532120', '塘栖枇杷');
    INSERT INTO `goods_info` VALUES ('230121', '临安山核桃');
    INSERT INTO `goods_info` VALUES ('250983', '西湖藕粉');
    INSERT INTO `goods_info` VALUES ('480071', '千岛湖鱼干');
    INSERT INTO `goods_info` VALUES ('580016', '天尊贡芽');
    INSERT INTO `goods_info` VALUES ('950013', '叫花童鸡');
    INSERT INTO `goods_info` VALUES ('152121', '火腿蚕豆');
    INSERT INTO `goods_info` VALUES ('230121', '杭州百鸟朝凤');



---

-- 创建门店信息纬度表

---

    DROP TABLE IF EXISTS `store_info`;
    CREATE TABLE `store_info` (
      `store_id` int NOT NULL,
      `store_name` varchar(50) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;



---

-- 插入商品信息样例数据

---

    INSERT INTO `store_info` VALUES ('313012', '莫干山店');
    INSERT INTO `store_info` VALUES ('313013', '定安路店');
    INSERT INTO `store_info` VALUES ('313014', '西湖银泰店');
    INSERT INTO `store_info` VALUES ('313015', '天目山店');
    INSERT INTO `store_info` VALUES ('313016', '凤起路店');
    INSERT INTO `store_info` VALUES ('313017', '南山路店');
    INSERT INTO `store_info` VALUES ('313018', '西溪湿地店');
    INSERT INTO `store_info` VALUES ('313019', '传媒学院店');
    INSERT INTO `store_info` VALUES ('313020', '西湖断桥店');
    INSERT INTO `store_info` VALUES ('313021', '保淑塔店');
    INSERT INTO `store_info` VALUES ('313022', '南宋御街店');
    INSERT INTO `store_info` VALUES ('313023', '河坊街店');

1.5 任务五：Spark作业

1-商品总的销售额（按天统计）数据输出

2-销售总额前5的门店排行数据输出

将数据写入到mysql表中，需要提前创建，创建语句如下：

    -- ----------------------------
    -- 创建商品总销售额表--goods_amount_count
    -- ----------------------------
    DROP TABLE IF EXISTS `goods_amount_count`;
    CREATE TABLE `goods_amount_count` (
      `amount_total` float NOT NULL,
      `sale_date` date PRIMARY KEY
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    -- ----------------------------
    -- 创建销售总额前5的门店排行表--amount_store_rank
    -- ----------------------------
    DROP TABLE IF EXISTS `amount_store_rank`;
    CREATE TABLE `amount_store_rank` (
      `store_id` int PRIMARY KEY,
      `store_name` varchar(50) DEFAULT NULL,
      `amount_total` float DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;


