from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.ml import Transformer
from pyspark.ml.param.shared import HasInputCol, HasOutputCol, Param, Params, TypeConverters
from pyspark import keyword_only
from pyspark.ml.util import DefaultParamsWritable, DefaultParamsReadable

class EmptyStringFilter(Transformer, HasInputCol, HasOutputCol, DefaultParamsWritable, DefaultParamsReadable):

    # Lọc bỏ các chuỗi rỗng và None khỏi mảng token
    @keyword_only
    def __init__(self, inputCol=None, outputCol=None):
        super(EmptyStringFilter, self).__init__()
        kwargs = self._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, inputCol=None, outputCol=None):
        kwargs = self._input_kwargs
        return self._set(**kwargs)

    def _transform(self, dataset):
        input_col = self.getInputCol()
        output_col = self.getOutputCol()
        
        def filter_empty_tokens(tokens):
            if tokens is None:
                return []
            # Lọc bỏ các token là None / chuỗi rỗng / khoảng trắng
            return [token for token in tokens if token and token.strip()]

        filter_udf = udf(filter_empty_tokens, ArrayType(StringType(), containsNull=False))
        
        return dataset.withColumn(output_col, filter_udf(dataset[input_col]))

    def copy(self, extra=None):
        if extra is None:
            extra = {}
        return self.defaultCopy(extra)