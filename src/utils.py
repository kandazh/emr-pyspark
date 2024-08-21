# utils.py

def calculate_statistics(data):
    """
    Calculate basic statistics from a DataFrame.

    :param data: A PySpark DataFrame
    :return: Dictionary with statistics
    """
    stats = {}
    stats['count'] = data.count()
    stats['mean'] = data.select('value').groupBy().mean().collect()[0][0]
    stats['max'] = data.select('value').groupBy().max().collect()[0][0]
    stats['min'] = data.select('value').groupBy().min().collect()[0][0]
    return stats

def transform_data(data):
    """
    Perform some transformation on the DataFrame.

    :param data: A PySpark DataFrame
    :return: Transformed DataFrame
    """
    return data.withColumn('transformed_value', data['value'] * 2)
