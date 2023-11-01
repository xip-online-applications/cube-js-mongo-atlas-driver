import R from 'ramda';
import {BaseFilter, BaseQuery} from "@cubejs-backend/schema-compiler";
import moment from 'moment-timezone';

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
export class MongoAtlasJDBCQuery extends BaseQuery {
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
    throw new Error('Not implemented');
  }

  timeStampCast(value: any): string {
    return `${value}::timestamp`;
  }


}
