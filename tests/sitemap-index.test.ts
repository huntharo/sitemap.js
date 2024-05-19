import { SitemapStream } from '../index';
import { tmpdir } from 'os';
import { resolve } from 'path';
import {
  existsSync,
  unlinkSync,
  createWriteStream,
  createReadStream,
} from 'fs';
import {
  SitemapIndexStream,
  SitemapAndIndexStream,
} from '../lib/sitemap-index-stream';
import { streamToPromise } from '../dist';
import { WriteStream } from 'node:fs';
import { createGunzip, createGzip } from 'zlib';

/* eslint-env jest, jasmine */
function removeFilesArray(files): void {
  if (files && files.length) {
    files.forEach(function (file) {
      if (existsSync(file)) {
        unlinkSync(file);
      }
    });
  }
}

const xmlDef = '<?xml version="1.0" encoding="UTF-8"?>';
describe('sitemapIndex', () => {
  it('build sitemap index - write', async () => {
    const expectedResult =
      xmlDef +
      '<?xml-stylesheet type="text/xsl" href="https://example.com/style.xsl"?>' +
      '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' +
      '<sitemap>' +
      '<loc>https://test.com/s1.xml</loc>' +
      '</sitemap>' +
      '<sitemap>' +
      '<loc>https://test.com/s2.xml</loc>' +
      '</sitemap>' +
      '</sitemapindex>';
    const smis = new SitemapIndexStream({
      xslUrl: 'https://example.com/style.xsl',
    });
    smis.write('https://test.com/s1.xml');
    smis.write('https://test.com/s2.xml');
    smis.end();
    const result = await streamToPromise(smis);

    expect(result.toString()).toBe(expectedResult);
  });

  it('build sitemap index - writeAsync', async () => {
    const expectedResult =
      xmlDef +
      '<?xml-stylesheet type="text/xsl" href="https://example.com/style.xsl"?>' +
      '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' +
      '<sitemap>' +
      '<loc>https://test.com/s1.xml</loc>' +
      '</sitemap>' +
      '<sitemap>' +
      '<loc>https://test.com/s2.xml</loc>' +
      '</sitemap>' +
      '</sitemapindex>';
    const smis = new SitemapIndexStream({
      xslUrl: 'https://example.com/style.xsl',
    });
    await smis.writeAsync('https://test.com/s1.xml');
    await smis.writeAsync('https://test.com/s2.xml');
    smis.end();
    const result = await streamToPromise(smis);

    await expect(async () =>
      smis.writeAsync('https://test.com/s1.xml')
    ).rejects.toThrow('write after end');

    expect(result.toString()).toBe(expectedResult);
  });

  it('build sitemap index with lastmodISO', async () => {
    const expectedResult =
      xmlDef +
      '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' +
      '<sitemap>' +
      '<loc>https://test.com/s1.xml</loc>' +
      '<lastmod>2018-11-26T00:00:00.000Z</lastmod>' +
      '</sitemap>' +
      '<sitemap>' +
      '<loc>https://test.com/s2.xml</loc>' +
      '<lastmod>2018-11-27T00:00:00.000Z</lastmod>' +
      '</sitemap>' +
      '<sitemap>' +
      '<loc>https://test.com/s3.xml</loc>' +
      '</sitemap>' +
      '</sitemapindex>';
    const smis = new SitemapIndexStream();
    smis.write({
      url: 'https://test.com/s1.xml',
      lastmod: '2018-11-26',
    });

    smis.write({
      url: 'https://test.com/s2.xml',
      lastmod: '2018-11-27',
    });

    smis.write({
      url: 'https://test.com/s3.xml',
    });
    smis.end();
    const result = await streamToPromise(smis);

    expect(result.toString()).toBe(expectedResult);
  });

  it('build sitemap index with lastmodDateOnly', async () => {
    const expectedResult =
      xmlDef +
      '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">' +
      '<sitemap>' +
      '<loc>https://test.com/s1.xml</loc>' +
      '<lastmod>2018-11-26</lastmod>' +
      '</sitemap>' +
      '<sitemap>' +
      '<loc>https://test.com/s2.xml</loc>' +
      '<lastmod>2018-11-27</lastmod>' +
      '</sitemap>' +
      '<sitemap>' +
      '<loc>https://test.com/s3.xml</loc>' +
      '</sitemap>' +
      '</sitemapindex>';
    const smis = new SitemapIndexStream({ lastmodDateOnly: true });
    smis.write({
      url: 'https://test.com/s1.xml',
      lastmod: '2018-11-26T00:00:00.000Z',
    });

    smis.write({
      url: 'https://test.com/s2.xml',
      lastmod: '2018-11-27T00:00:00.000Z',
    });

    smis.write({
      url: 'https://test.com/s3.xml',
    });
    smis.end();
    const result = await streamToPromise(smis);

    expect(result.toString()).toBe(expectedResult);
  });
});

describe('sitemapAndIndex', () => {
  let targetFolder: string;

  beforeEach(() => {
    targetFolder = tmpdir();
    removeFilesArray([
      resolve(targetFolder, `./sitemap-0.xml`),
      resolve(targetFolder, `./sitemap-1.xml`),
      resolve(targetFolder, `./sitemap-2.xml`),
      resolve(targetFolder, `./sitemap-3.xml`),
      resolve(targetFolder, `./sitemap-4.xml`),
      resolve(targetFolder, `./sitemap-0.xml.gz`),
      resolve(targetFolder, `./sitemap-1.xml.gz`),
      resolve(targetFolder, `./sitemap-2.xml.gz`),
      resolve(targetFolder, `./sitemap-3.xml.gz`),
      resolve(targetFolder, `./sitemap-4.xml.gz`),
    ]);
  });

  afterEach(() => {
    removeFilesArray([
      resolve(targetFolder, `./sitemap-0.xml`),
      resolve(targetFolder, `./sitemap-1.xml`),
      resolve(targetFolder, `./sitemap-2.xml`),
      resolve(targetFolder, `./sitemap-3.xml`),
      resolve(targetFolder, `./sitemap-4.xml`),
      resolve(targetFolder, `./sitemap-0.xml.gz`),
      resolve(targetFolder, `./sitemap-1.xml.gz`),
      resolve(targetFolder, `./sitemap-2.xml.gz`),
      resolve(targetFolder, `./sitemap-3.xml.gz`),
      resolve(targetFolder, `./sitemap-4.xml.gz`),
    ]);
  });

  it('writes both a sitemap and index - countLimit', async () => {
    const baseURL = 'https://example.com/sub/';

    const sms = new SitemapAndIndexStream({
      countLimit: 1,
      getSitemapStream: (i: number): [string, SitemapStream, WriteStream] => {
        const sm = new SitemapStream();
        const path = `./sitemap-${i}.xml`;

        const ws = sm.pipe(createWriteStream(resolve(targetFolder, path)));
        return [new URL(path, baseURL).toString(), sm, ws];
      },
    });
    sms.write('https://1.example.com/a');
    sms.write('https://2.example.com/a');
    sms.write('https://3.example.com/a');
    sms.write('https://4.example.com/a');
    sms.end();
    const indexStr = (await streamToPromise(sms)).toString();
    expect(indexStr).toContain(`${baseURL}sitemap-0`);
    expect(indexStr).toContain(`${baseURL}sitemap-1`);
    expect(indexStr).toContain(`${baseURL}sitemap-2`);
    expect(indexStr).toContain(`${baseURL}sitemap-3`);
    expect(indexStr).not.toContain(`${baseURL}sitemap-4`);
    expect(existsSync(resolve(targetFolder, `./sitemap-0.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-1.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-2.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-3.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-4.xml`))).toBe(false);
    {
      const xml = await streamToPromise(
        createReadStream(resolve(targetFolder, `./sitemap-0.xml`))
      );
      expect(xml.toString()).toContain('https://1.example.com/a');
    }
    {
      const xml = await streamToPromise(
        createReadStream(resolve(targetFolder, `./sitemap-3.xml`))
      );
      expect(xml.toString()).toContain('https://4.example.com/a');
    }
  });

  it.only('writes both a sitemap and index - countLimit - gzip', async () => {
    const baseURL = 'https://example.com/sub/';

    const sms = new SitemapAndIndexStream({
      countLimit: 1,
      getSitemapStream: (i: number): [string, SitemapStream, WriteStream] => {
        const sm = new SitemapStream();
        const path = `./sitemap-${i}.xml`;

        const ws = createWriteStream(resolve(targetFolder, path));
        sm.pipe(createGzip()).pipe(ws);
        return [new URL(path, baseURL).toString(), sm, ws];
      },
    });
    sms.write('https://1.example.com/a');
    sms.write('https://2.example.com/a');
    sms.write('https://3.example.com/a');
    sms.write('https://4.example.com/a');
    sms.end();
    const indexStr = (await streamToPromise(sms)).toString();
    expect(indexStr).toContain(`${baseURL}sitemap-0`);
    expect(indexStr).toContain(`${baseURL}sitemap-1`);
    expect(indexStr).toContain(`${baseURL}sitemap-2`);
    expect(indexStr).toContain(`${baseURL}sitemap-3`);
    expect(indexStr).not.toContain(`${baseURL}sitemap-4`);
    expect(existsSync(resolve(targetFolder, `./sitemap-0.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-1.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-2.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-3.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-4.xml`))).toBe(false);
    {
      const xml = await streamToPromise(
        createReadStream(resolve(targetFolder, `./sitemap-0.xml`)).pipe(
          createGunzip()
        )
      );
      expect(xml.toString()).toContain('https://1.example.com/a');
    }
    {
      const xml = await streamToPromise(
        createReadStream(resolve(targetFolder, `./sitemap-3.xml`)).pipe(
          createGunzip()
        )
      );
      expect(xml.toString()).toContain('https://4.example.com/a');
    }
  });

  it('writes both a sitemap and index - byteLimit', async () => {
    const baseURL = 'https://example.com/sub/';

    const sms = new SitemapAndIndexStream({
      countLimit: 10,
      byteLimit: 400,
      getSitemapStream: (i: number): [string, SitemapStream, WriteStream] => {
        const sm = new SitemapStream();
        const path = `./sitemap-${i}.xml`;

        const ws = sm.pipe(createWriteStream(resolve(targetFolder, path)));
        return [new URL(path, baseURL).toString(), sm, ws];
      },
    });
    sms.write('https://1.example.com/a');
    sms.write('https://2.example.com/a');
    sms.write('https://3.example.com/a');
    sms.write('https://4.example.com/a');
    sms.end();
    const index = (await streamToPromise(sms)).toString();
    expect(index).toContain(`${baseURL}sitemap-0`);
    expect(index).toContain(`${baseURL}sitemap-1`);
    expect(index).toContain(`${baseURL}sitemap-2`);
    expect(index).toContain(`${baseURL}sitemap-3`);
    expect(index).not.toContain(`${baseURL}sitemap-4`);
    expect(existsSync(resolve(targetFolder, `./sitemap-0.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-1.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-2.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-3.xml`))).toBe(true);
    expect(existsSync(resolve(targetFolder, `./sitemap-4.xml`))).toBe(false);
    const xml = await streamToPromise(
      createReadStream(resolve(targetFolder, `./sitemap-0.xml`))
    );
    expect(xml.toString()).toContain('https://1.example.com/a');
  });
});
