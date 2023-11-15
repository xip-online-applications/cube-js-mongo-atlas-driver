import R from 'ramda';
import {BaseFilter, BaseQuery} from "@cubejs-backend/schema-compiler";
import moment from 'moment-timezone';

const GRANULARITY_TO_INTERVAL = {
  day: 'day',
  week: 'week',
  hour: 'hour',
  minute: 'minute',
  second: 'second',
  month: 'month',
  quarter: 'quarter',
  year: 'year'
};

class MongoAtlasJDBCFilter extends BaseFilter {
  castParameter() {
    if (this.definition().type === 'boolean') {
      return 'CAST(? AS BOOL)';
    } else if (this.measure || this.definition().type === 'number') {
      return 'CAST(? AS DOUBLE)';
    }

    return '?';
  }
}

/**
 * @override
 */
export class MongoAtlasQuery extends BaseQuery {
  newFilter(filter: BaseFilter) {
    return new MongoAtlasJDBCFilter(this, filter);
  }

  public groupByClause() {
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(
        dimensionsForSelect.map((s: any) => s.selectColumns() && s.aliasName())
    )
        .filter(s => !!s);

    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }

  convertTz(field: string): string {
    const tz = moment().tz(this.timezone);
    // TODO respect day light saving
    const [hour, minute] = tz.format('Z').split(':');
    const [hourInt, minuteInt] = [parseInt(hour, 10), parseInt(minute, 10) * Math.sign(parseInt(hour, 10))];
    let result = field;
    if (hourInt !== 0) {
      result = `DATEADD(HOUR, ${hourInt}, ${result})`;
    }
    if (minuteInt !== 0) {
      result = `DATEADD(HOUR, ${minuteInt}, ${result})`;
    }
    return result;
  }

  numer(value: string){
    return `${value}::timestamp`;
  }

  timeGroupedColumn(granularity: keyof typeof GRANULARITY_TO_INTERVAL, dimension: string) {
    return `DATETRUNC(${GRANULARITY_TO_INTERVAL[granularity]}, ${dimension})`;
  }

  timeStampCast(value: any): string {
    return `${value}::timestamp`;
  }
}
