import R from 'ramda';
import {BaseQuery} from "@cubejs-backend/schema-compiler";
import moment from 'moment-timezone';

/**
 * @override
 */
export class MongoAtlasJDBCQuery extends BaseQuery {

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
