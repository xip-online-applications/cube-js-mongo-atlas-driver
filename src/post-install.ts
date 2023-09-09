import 'source-map-support/register';

import { displayCLIError } from '@cubejs-backend/shared';

import fs from 'fs';
import path from 'path';
import { downloadJDBCDriver } from './installer';

(async () => {
  try {
    if (!fs.existsSync(path.join(__dirname, 'download', 'MongoAtlasJDBC.jar'))) {
      await downloadJDBCDriver();
    }
  } catch (e: any) {
    await displayCLIError(e, 'Cube.js MongoDB Atlas JDBC Installer');
  }
})();
