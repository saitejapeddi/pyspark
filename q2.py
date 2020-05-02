import findspark
findspark.init()
from pyspark import SparkContext
from pyspark import SparkConf


def final(line):
    usr1_fname=line[1][0][0][0]
    usr1_lname=line[1][0][0][1]
    usr1_addr=line[1][0][0][2]
    usr2_fname=line[1][1][0]
    usr2_lname=line[1][1][1]
    usr2_addr=line[1][1][2]
    mutual=line[1][0][1]
    return "{}\t{}\t{}\t{}\t{}\t{}\t{}".format(mutual,usr1_fname,usr1_lname,usr1_addr,usr2_fname,usr2_lname,usr2_addr)

def pairs(line):
    user1 = line[0].strip()
    friends = line[1]
    if user1 != '':
        all_pairs = []
        for friend in friends:
            friend = friend.strip()
            if friend != '':
                if float(friend) < float(user1):
                    pairs = (friend + "," + user1, set(friends))
                else:
                    pairs = (user1 + "," + friend, set(friends))
                all_pairs.append(pairs)
        return all_pairs


def joining(line):
    new = line[1][0][0]
    mutual= line[1][0][1]
    user1_fname  =line[1][1][0]
    user1_lname = line[1][1][1]
    user1_addr = line[1][1][2]

    return new,((user1_fname,user1_lname,user1_addr),mutual)



if __name__ == "__main__":
    conf = SparkConf().setMaster("local").setAppName("q2")
    sc = SparkContext(conf=conf)
    friends = sc.textFile("C:/Users/psait/Desktop/bda/soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t")).filter(lambda x: len(x) == 2).map(lambda x: [x[0], x[1].split(",")]).flatMap(pairs).reduceByKey(lambda x, y: x.intersection(y))

    user_details = sc.textFile("C:/Users/psait/Desktop/bda/userdata.txt").map(lambda line: line.split(","))
    user_details_cleaned=user_details.map(lambda x: (x[0],(x[1],x[2],x[3])))
    friend_pairs = friends
    common_friends = friend_pairs
    x=friends.map(lambda x:(x[0].split(",")[0],(x[0].split(",")[1],len(x[1]))))
    top_10=x.top(10,key=lambda x: x[1][1])
    y=sc.parallelize(top_10)
    joined_tab1 =y.join(user_details_cleaned)
    join_tab_formatted=joined_tab1.map(joining)
    final_join=join_tab_formatted.join(user_details_cleaned)
    final_res=final_join.map(final)
    final_res.coalesce(1).saveAsTextFile("C:/Users/psait/Desktop/bda/q2.txt")