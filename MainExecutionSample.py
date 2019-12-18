from pysparkprocessor.wrappers import LoggerWrapper
import configparser
import os
from pysparkprocessor.adapters import PipeExecutor
from pysparkprocessor.pipelines import CommonHiveReadWritePipeline
from pysparkprocessor.pipelines import SampleDataFrameMapperPipeline

# Initialize logger
logger = LoggerWrapper.init_logger(__name__)

# Get current script directory
main_dir = os.path.dirname(os.path.abspath(__file__))
logger.debug("Executing main script in " + main_dir + " directory")

# Load config from config file using configparser lib
logger.debug("Loading configuration from config file")
config = configparser.ConfigParser()
config.read(os.path.join(main_dir, "../config/config.ini"))
logger.debug("Configuration loaded")

# Load pyspark lib
logger.debug("Calling PysparkWrapper to load pyspark lib")
from pysparkprocessor.wrappers import PysparkWrapper
PysparkWrapper.init_pyspark()
logger.debug("PysparkWrapper successfully loaded pyspark lib")

# Initialize HiveContext for sql read/write purpose
logger.debug("Calling PysparkWrapper to initialize hive context")
PysparkWrapper.init_pyspark()
hive_context = PysparkWrapper.get_hivecontext("SampleMainPipeline")
logger.debug("Hive context initialized")

# Execute pipeline operations for data processing
logger.debug("Executing hive pipelines to process data")
result = PipeExecutor.get_execution_result(
  PipeExecutor.execute(
    *[
      CommonHiveReadWritePipeline.read_from_table, 
      SampleDataFrameMapperPipeline.map_read_output_to_write_input, 
      CommonHiveReadWritePipeline.insert_to_table
    ], 
    **{
        "input_table":"default.test10", 
        "output_table":"default.test10", 
        "input":None, 
        "output":None,
        "sqlcontext": hive_context
    }
  )
)
logger.debug("Finish executing pipeline")
result["output"].show()
