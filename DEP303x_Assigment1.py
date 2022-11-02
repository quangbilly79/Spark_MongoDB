from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master('local[*]') \
        .appName('MyApp') \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config('spark.mongodb.input.uri', 'mongodb://localhost:27017/Asm1Dep303') \
        .config('spark.mongodb.output.uri', 'mongodb://localhost:27017/Asm1Dep303') \
        .config("spark.driver.memory", "6g")\
        .enableHiveSupport()\
        .getOrCreate()

    # Đọc Collection Questions
    dfQ = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource')\
        .option("uri", "mongodb://localhost:27017/Asm1Dep303.Questions")\
        .load()
    # Đọc Collection Answers
    dfA = spark.read \
        .format('com.mongodb.spark.sql.DefaultSource') \
        .option("uri", "mongodb://localhost:27017/Asm1Dep303.Answers") \
        .load()

    # Xử lý Schema cho Questions
    dfQ1 = dfQ.drop("_id")\
        .withColumn("ClosedDate",
                    to_date(
                        when(col("ClosedDate") == "NA", None)
                        .otherwise(substring_index("ClosedDate", "T",1)),
                        "yyyy-MM-dd"))\
        .withColumn("OwnerUserId", expr('case OwnerUserId when "NA" then null else OwnerUserId end').cast(IntegerType()))\
        .withColumn("CreationDate", to_date(substring_index("CreationDate", "T",1), "yyyy-MM-dd"))\
        .withColumn("Id", dfQ.Id.cast(IntegerType())) \
        .withColumn("Score", dfQ.Score.cast(IntegerType()))

    #dfQ1.printSchema()
    # root
    # | -- Body: string(nullable=true)
    # | -- ClosedDate: date(nullable=true) # Có 1 số giá trị "NA" đổi thành None rồi chuyển thành Date, giá trị dạng 2008-08-01T13:57:07Z thì đổi thành 2008-08-01
    # | -- CreationDate: date(nullable=true) # Giá trị dạng 2008-08-01T13:57:07Z thì đổi thành 2008-08-01
    # | -- Id: integer(nullable=true)
    # | -- OwnerUserId: integer(nullable=true) # Có 1 số giá trị "NA" đổi thành null rồi chuyển thành int
    # | -- Score: integer(nullable=true)
    # | -- Title: string(nullable=true)
    # Xử lý Schema cho Answers
    dfA1 = dfA.drop("_id")\
        .withColumn("OwnerUserId", expr('case OwnerUserId when "NA" then null else OwnerUserId end').cast(IntegerType()))\
        .withColumn("ParentId", dfA.ParentId.cast(IntegerType()))\
        .withColumn("CreationDate", to_date(substring_index("CreationDate", "T",1), "yyyy-MM-dd"))\
        .withColumn("Id", dfA.Id.cast(IntegerType())) \
        .withColumn("Score", dfA.Score.cast(IntegerType()))

    #dfA1.printSchema()
    # root
    # | -- Body: string(nullable=true)
    # | -- CreationDate: date(nullable=true)
    # | -- Id: integer(nullable=true)
    # | -- OwnerUserId: integer(nullable=true)
    # | -- ParentId: integer(nullable=true)
    # | -- Score: integer(nullable=true)

    #### 1. Tính số lần xuất hiện của các ngôn ngữ lập trình trong body Questions
    # col.rlike ~ re.search(), trả về boolean value nếu giá trị trong col match
    # Dùng Case When kết hợp với GroupBy. Nếu match C++ thì trả về giá trị là C++, sau đó group theo giá trị này
    dfQProgramLanguage = dfQ1.groupBy(when(dfQ1.Body.rlike("C#"), "C#")
                                      .when(dfQ1.Body.rlike("C\+\+"), "C++")
                                      .when(dfQ1.Body.rlike("CSS"), "CSS")
                                      .when(dfQ1.Body.rlike("HTML"), "HTML")
                                      .when(dfQ1.Body.rlike("PHP"), "PHP")
                                      .when(dfQ1.Body.rlike("SQL"), "SQL")
                                      .when(dfQ1.Body.rlike("Go"), "Go")
                                      .when(dfQ1.Body.rlike("Ruby"), "Ruby")
                                      .when(dfQ1.Body.rlike("Python"), "Python")
                                      .when(dfQ1.Body.rlike("Java"), "Java")
                                      .otherwise("Other")
                                      ).count()
    # Đổi tên cột đầu với withColumnRenamed(df.column[0], "new name") thành Programming Language cho dễ nhìn
    print('Yeu cau 1:')
    dfQProgramLanguageFinal = dfQProgramLanguage.withColumnRenamed(dfQProgramLanguage.columns[0], "Programming Language").show()

    # +--------------------+------+
    # |Programming Language| count|
    # +--------------------+------+
    # |                  C#| 25037|
    # |                 C++| 17217|
    # |               Other|967003|
    # |                 CSS| 22416|


    #### 2. Tìm các domain được sử dụng nhiều nhất trong các câu hỏi
    # Dùng regexp_extract(Col, pattern, groupNumber) để Extract group đầu tiên (và cx là duy nhất) match với pattern như bên dưới
    # vd .....<a href="http://stackoverflow.com/questio... thì sẽ match stackoverflow.com
    # Dùng regex_extract_all để lấy tất cả các match thành 1 array, xong explode
    pattern = r'href=\"http://([\w\.]+)/'
    dfQdomain = dfQ1.withColumn("domain", regexp_extract(dfQ1.Body, pattern, 1))\
                    .select("domain")\
                    .filter(col("domain") != "")
    # Tiếp theo là GroupBy xong count
    print('Yeu cau 2:')
    dfQdomainFinal = dfQdomain.groupBy("domain").count().orderBy(col("count").desc()).show()
    # +--------------------+-----+
    # | domain | count |
    # +--------------------+-----+
    # | stackoverflow.com | 41672 |
    # | i.stack.imgur.com | 25533 |
    # | jsfiddle.net | 14999 |
    # | msdn.microsoft.com | 3621 |


    ##### 3. Tính tổng điểm của User theo từng ngày
    # Bạn cần biết được xem đến ngày nào đó thì User đạt được bao nhiêu điểm

    # Partition theo OwnerUserId, tính tổng cộng dồn của Score (unboundedPrecced - currentRow) theo CreationDate
    windowScore = Window.partitionBy(dfQ1.OwnerUserId).orderBy(dfQ1.CreationDate)
    print('Yeu cau 3:')
    dfQsccore = dfQ1.select("OwnerUserId", "CreationDate", sum("Score").over(windowScore).alias("TotalScore"))\
        .orderBy("OwnerUserId", col("CreationDate").asc())\
        .filter(dfQ1.OwnerUserId.isNotNull())\
        .show()
    # +-----------+------------+----------+
    # | OwnerUserId | CreationDate | TotalScore |
    # +-----------+------------+----------+
    # | 1 | 2008 - 11 - 26 | 10 |
    # | 1 | 2009 - 01 - 08 | 30 |
    # | 1 | 2009 - 10 - 08 | 58 |

    #### 4. Tính tổng số điểm mà User đạt được trong một khoảng thời gian
    # Ở yêu cầu này, bạn sẽ cần tính tổng điểm mà User đạt được khi đặt câu hỏi trong một khoảng thời gian

    startDate = "2008-01-01"
    endDate = "2009-01-01"
    # Filter CreationDate xong groupBy theo OwnerUserId và tính tổng Score thôi
    # Ngoài ra lọc bớt giá trị Null cho đẹp
    print('Yeu cau 4:')
    dfQScore1 = dfQ1.filter(dfQ1.CreationDate.between(startDate,endDate) & dfQ1.OwnerUserId.isNotNull())\
        .groupBy(dfQ1.OwnerUserId)\
        .agg(sum(dfQ1.Score)).alias("TotalScore")\
        .orderBy(dfQ1.OwnerUserId)\
        .show()

    # -----------+----------+
    # | OwnerUserId | TotalScore |
    # +-----------+----------+
    # | 1 | 10 |
    # | 4 | 4 |
    # | 5 | 0 |

    #### 5. Tìm các câu hỏi có nhiều câu trả lời
    # Một câu hỏi tốt sẽ được tính số lượng câu trả lời của câu hỏi đó,
    # nếu như câu hỏi có nhiều hơn 5 câu trả lời thì sẽ được tính là tốt. Bạn sẽ cần tìm xem có bao nhiêu câu hỏi đang được tính là tốt,

    # Tạo và sử dụng Database
    #spark.sql("CREATE DATABASE IF NOT EXISTS asm1_db")
    spark.sql("USE asm1_db")


    #Tạo 2 bảng tg ứng với Answer và Question, chia làm 6 buckets dựa vào Id và ParentId (Id của câu hỏi trong Answer)
    dfQ1.write.bucketBy(2, "Id").mode("overwrite").saveAsTable("asm1_db.tableQ1")
    dfA1.write.bucketBy(2, "ParentId").mode("overwrite").saveAsTable("asm1_db.tableA1")

    # SQL query để lấy các ttin theo yêu cầu
    statement = '''
    select q.Id, count(*) as count
    from tableq1 as q join tablea1 as a on q.Id = a.ParentId
    group by q.Id
    having count(*) > 5
    order by q.Id
    '''
    dfMoreThan5 = spark.sql(statement)
    print('Yeu cau 5:')
    dfMoreThan5.show()
    # +----+-----+
    # | Id | count |
    # +----+-----+
    # | 180 | 9 |
    # | 260 | 9 |
    # | 330 | 10 |
    # | 580 | 14 |
    # | 650 | 6 |
    # | 930 | 7 |

    #### 6. Tìm các Active User
    # Một User được tính là Active sẽ cần thỏa mãn một trong các yêu cầu sau:
    # Có nhiều hơn 50 câu trả lời hoặc tổng số điểm đạt được khi trả lời lớn hơn 500. (1)
    # Có nhiều hơn 5 câu trả lời ngay trong ngày câu hỏi được tạo. (2)

    # Xử lý điều kiện (2). Làm tương tự như yêu cầu 5, thêm đk Join là q.CreationDate = a.CreationDate
    # Bỏ những UserId là null đi nữa
    # và Lấy những UserId thỏa mãn đk
    statement2 = '''
        select q.OwnerUserId
        from tableq1 as q join tablea1 as a on q.Id = a.ParentId and  a.CreationDate = q.CreationDate
        where q.OwnerUserId is not null
        group by q.Id, q.OwnerUserId
        having count(*) > 5
        '''
    cond2 = spark.sql(statement2)
    # +-----+
    # | OwnerUserId |
    # +-----+
    # | 1 |
    # | 4 |
    # | 33 |
    # | 35 |

    # Xử lý điều kiện (1). Lấy những User Id thỏa mãn đk1
    statement1 = '''
    select OwnerUserId
    from tablea1
    where OwnerUserId is not null
    group by OwnerUserId
    having count(*) > 50 or sum(Score) > 500
    '''

    cond1 = spark.sql(statement1)
    # +-----------+
    # | OwnerUserId |
    # +-----------+
    # | 22656 |
    # | 1144035 |

    # Vì thỏa mã 1 trong 2 điều kiện nên ta sẽ union 2 bảng và thêm đk distinct()
    condAll = cond2.union(cond1).distinct().orderBy("OwnerUserId")
    print('Yeu cau 6:')
    condAll.show()
    # +-----------+
    # | OwnerUserId |
    # +-----------+
    # | 1 |
    # | 4 |
    # | 13 |
    # | 29 |

    # Test thử việc Write kq vào collection trong Mongodb
    # condAll.write.format('com.mongodb.spark.sql.DefaultSource')\
    #     .mode('overwrite')\
    #     .option("spark.mongodb.output.uri", "mongodb://localhost:27017/Asm1Dep303.output")\
    #     .save()