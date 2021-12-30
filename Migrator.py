from helper.connector import Connector
from helper.config_reader import ConfigReader
from helper.loader import Loader
import sys

class Migrator():
    def __init__(self):
        config = ConfigReader.load_config()
        connector_obj = Connector(config)
        ext_con = connector_obj.establish_connection('EXTRACTION')
        com_con = connector_obj.establish_connection('COMMIT')
        helper_obj = Loader(ext_con, com_con)

        # index_name = self.ext_con['INDEX'] if self.ext_con['INDEX'] != None else self.com_con['INDEX']
        # index_size_es = helper_obj.find_stop_point() #To be transferred

        try:
            resume_point = self.find_resume_point()
        except:
            resume_point = 0

        # try:
        #     self.es_mark=self.find_resume_point_es()
        # except:
        #     self.es_mark=0

        try:
            stop_mark = helper_obj.find_remaining_count()
        except:
            # TBD: Handle Exception
            stop_mark = 0

        if stop_mark == 0:
            print ('Transfer Complete!!!')
            sys.exit()

        while stop_mark != 0:
            input_data = helper_obj.read_data()
            output_data = helper_obj.write_data(input_data)
            resume_point = helper_obj.find_resume_point()
            stop_mark = helper_obj.find_remaining_count()
            print ('%s - Partition transfer completed'%(str(resume_point)))
            print ('%s - documents left'%(str(stop_mark)))
        # print ('Transfer Complete!!!')
        return None

