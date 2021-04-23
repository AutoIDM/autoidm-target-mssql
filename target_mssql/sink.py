
from singer_sdk.target_sink_base import TargetSinkBase
from singer_sdk_target import TargetBase

class MSSSQLTargetSink(TargetSinkBase):

  def flush_records(self, records_to_load: Iterable[Dict], expected_row_count: Optional[int]):
    raise NotImplementedError("Not ready ready dude")
    

