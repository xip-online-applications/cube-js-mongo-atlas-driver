import path from 'path';
import fs from 'fs';
import * as https from 'https';

export async function downloadJDBCDriver(): Promise<string | null> {
  console.log('Downloading Mongo Atlas JDBC driver...');

  const url: string = 'https://repo1.maven.org/maven2/org/mongodb/mongodb-jdbc/2.0.3/mongodb-jdbc-2.0.3-all.jar';
  const destination: string = path.resolve(path.join(__dirname, '..', 'download', 'MongoAtlasJDBC.jar'));

  const dir = path.dirname(destination);
  await fs.promises.mkdir(dir, { recursive: true });
  const file = fs.createWriteStream(destination);

  https.get(url, response => {
    response.pipe(file);
    file.on('finish', () => {
      file.close(); // Close the file when download is complete
      console.log('Download complete...');
    });
  })
    .on('error', error => {
      fs.unlink(destination, err => {
        if (err) {
          console.error(`Could not delete file: ${err.message}`);
        }
      }); // Delete the file async
      console.error(`An error occurred: ${error.message}`);
    });

  return path.resolve(path.join(__dirname, '..', 'download', 'MongoAtlasJDBC.jar'));
}
