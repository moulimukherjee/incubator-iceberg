package org.apache.iceberg.transforms;

import org.apache.iceberg.expressions.BoundPredicate;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

public class DoubleTimestamp implements Transform<Double, Integer> {
  final OffsetDateTime EPOCH = Instant.ofEpochSecond(0).atOffset(ZoneOffset.UTC);

  @Override
  public Integer apply(Double value) {
    OffsetDateTime timestamp = Instant
      .ofEpochSecond(value.longValue())
      .atOffset(ZoneOffset.UTC);
    Integer year = Long.valueOf(ChronoUnit.DAYS.between(EPOCH, timestamp)).intValue();
    return year;
  }

  @Override
  public boolean canTransform(Type type) {
    return type.typeId() == Type.TypeID.DOUBLE;
  }

  @Override
  public Type getResultType(Type sourceType) {
    return Types.IntegerType.get();
  }

  @Override
  public UnboundPredicate<Integer> project(String fieldName, BoundPredicate<Double> pred) {
    if (pred.op() == Expression.Operation.NOT_NULL || pred.op() == Expression.Operation.IS_NULL) {
      return Expressions.predicate(pred.op(), fieldName);
    }
    return ProjectionUtil.truncateDouble(fieldName, pred, this);
  }

  @Override
  public UnboundPredicate<Integer> projectStrict(String name, BoundPredicate<Double> predicate) {
    return null;
  }

  @Override
  public String toHumanString(Integer dayOrdinal) {
    OffsetDateTime day = EPOCH.plusDays(dayOrdinal);
    return String.format("%04d-%02d-%02d",
      day.getYear(), day.getMonth().getValue(), day.getDayOfMonth());
  }

  @Override
  public String toString() {
    return "day";
  }
};