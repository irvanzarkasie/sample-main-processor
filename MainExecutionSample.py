from pysparkprocessor.wrappers import LoggerWrapper
import configparser
import os
from pysparkprocessor.wrappers import PysparkWrapper
from pysparkprocessor.adapters import PipeExecutor
from pysparkprocessor.pipelines import CommonHiveReadWritePipeline
from pysparkprocessor.pipelines import SampleDataFrameMapperPipeline

# Initialize logger
logger = LoggerWrapper.init_logger(__name__)

# Bootstrap PysparkProcessor
hive_context = PysparkWrapper.init_processor("SampleMainPipeline")

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
        "input_table":"default.test100",
        "output_table":"default.test100",
        "input":None,
        "output":None,
        "sqlcontext": hive_context
    }
  )
)
logger.debug("Finish executing pipeline")
result["output"].show()