
class GenerateRequests:
    
    @staticmethod
    def go():
        URL = "http://api.skymapper.nci.org.au/public/tap"
        TABLE_NAME = "dr1.dr1p1_master"
        with open("download_dr1p1_master.sh", 'w+') as f:
            for batch_min, batch_max in GenerateRequests.generate_batch_queue(0, 10000000, 500000):
                f.write(
                    GenerateRequests.generate_command(
                        URL,
                        GenerateRequests.generate_range_query(TABLE_NAME,
                                                            "object_id",
                                                            batch_min,
                                                            batch_max
                        ),
                        GenerateRequests.generate_filename(TABLE_NAME, batch_min, batch_max)
                    )
                )
        
    
    @staticmethod
    def generate_command(url, query, filepath):
        return(f"java -jar stilts.jar "
                f"tapquery tapurl='{url}' "
                f"adql='{query}' "
                f"out='{filepath}'\n")
    
    @staticmethod
    def generate_batch_queue(the_min, the_max, batch_size):
        queue = [[the_min, the_min]]
        batch_min = the_min
        batch_max = the_min

        iterations = int((the_max - the_min) / batch_size)
        for i in range(iterations):
            batch_min = batch_max
            batch_max = batch_max + batch_size
            queue.append([batch_min, batch_max])
        
        remainder = (the_max - the_min) % batch_size
        if remainder != 0:
            queue.append([batch_max, batch_max + remainder])
        return queue
    
    @staticmethod
    def generate_filename(table, batch_min, batch_max):
        return f"{table}_{batch_min}-{batch_max}.fits"

    @staticmethod
    def generate_range_query(table, id_name, batch_min, batch_max):
        return (f"SELECT * FROM {table} "
                f"WHERE {id_name}>{batch_min} AND {id_name}<={batch_max}")
    
    @staticmethod
    def generate_id_query(table, id_name, id_value):
        return f"SELECT * FROM {table} WHERE {id_name}={id_value}"

if __name__ == "__main__":
    GenerateRequests.go()