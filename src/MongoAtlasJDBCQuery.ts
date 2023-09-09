import {MongoBiQuery} from '@cubejs-backend/schema-compiler/dist/src/adapter/MongoBiQuery';
import R from 'ramda';

/**
 * @override
 */
export class MongoAtlasJDBCQuery extends MongoBiQuery {

  public groupByClause() {
    const dimensionsForSelect = this.dimensionsForSelect();
    const dimensionColumns = R.flatten(
        dimensionsForSelect.map((s: any) => s.selectColumns() && s.aliasName())
    )
        .filter(s => !!s);

    return dimensionColumns.length ? ` GROUP BY ${dimensionColumns.join(', ')}` : '';
  }
}
