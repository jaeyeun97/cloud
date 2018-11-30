import numpy as np
# import matplotlib.pyplot as plt
from scipy.optimize import curve_fit, minimize


# xs = [filesize / nodenum]
def getseconds(xs, a, b):
    return np.multiply(a, np.divide([x[0] for x in xs], [x[1] for x in xs])) + b


# xs = spark node number
def getmaxsecond(xs, total_num, spark_size, custom_size, sopts, copts):
    return np.maximum(getseconds([[spark_size, xs[0]]], sopts[0], sopts[1])[0],
                      getseconds([[custom_size, total_num-xs[0]]], copts[0], copts[1])[0])


spark = dict()
custom = dict()
for i in [2, 5, 8]:
    spark[i] = dict()
    custom[i] = dict()

with open('spark.csv', 'r') as f:
    for line in f:
        ts = line.rstrip().split(',')
        spark[int(ts[0])][int(ts[1])] = int(ts[2])

with open('custom.csv', 'r') as f:
    for line in f:
        ts = line.rstrip().split(',')
        custom[int(ts[0])][int(ts[1])] = int(ts[2])

xs = [[j, i] for i in [2, 5, 8] for j in [200, 400, 500]]
y = [min(spark[x[1]][x[0]], custom[x[1]][x[0]]) for x in xs]
spark_y = [spark[x[1]][x[0]] for x in xs]
custom_y = [custom[x[1]][x[0]] for x in xs]

sopts, scov = curve_fit(getseconds, xs, spark_y)
copts, ccov = curve_fit(getseconds, xs, custom_y)


def spark(total_num, custom_file_size, spark_file_size):
    result = minimize(getmaxsecond, [5], args=(total_num, spark_file_size, custom_file_size, sopts, copts), bounds=((2, total_num-2),))
    spark_num = round(result.x[0])
    # custom_num = total_num - spark_num

    return int(spark_num)
    #print('Spark: {}, Custom: {}'.format(spark_num, custom_num))

# x = np.linspace(100, 1000, 1000)
# spark_y = getseconds(x, sopts[0], sopts[1])
# custom_y = getseconds(x, copts[0], copts[1])
