import { promisify } from 'util';
import {
  finished,
  pipeline,
  Writable,
  Transform,
  TransformCallback,
} from 'stream';
import { createGzip, gunzipSync, Gzip } from 'zlib';
import {
  SitemapStream,
  closetag,
  streamToPromise,
} from '../lib/sitemap-stream';
import { createReadStream, createWriteStream } from 'fs';
import { ByteLimitExceededError, CountLimitExceededError } from '../lib/errors';
import { SitemapItemLoose } from '../lib/types';
import path from 'path';

const finishedAsync = promisify(finished);

const charset = '<?xml version="1.0" encoding="UTF-8"?>';
const urlset = '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"';
const minimumns = charset + urlset;
const news = ' xmlns:news="http://www.google.com/schemas/sitemap-news/0.9"';
const xhtml = ' xmlns:xhtml="http://www.w3.org/1999/xhtml"';
const image = ' xmlns:image="http://www.google.com/schemas/sitemap-image/1.1"';
const video = ' xmlns:video="http://www.google.com/schemas/sitemap-video/1.1"';
const urlsetMins = urlset + news + xhtml + image + video + '>';
const preamble = minimumns + news + xhtml + image + video + '>';

export class SitemapStreamErrorSuppression extends Transform {
  public _transform(
    item: SitemapItemLoose,
    encoding: BufferEncoding,
    callback: TransformCallback
  ): void {
    callback(null, item);
  }
}

export class GzipSourceErrorSuppression extends Transform {
  private gzip: Gzip;

  constructor() {
    super();
    this.gzip = createGzip();
    this.gzip.on('data', (chunk) => {
      this.push(chunk);
    });
  }

  public _transform(
    chunk: any,
    encoding: BufferEncoding,
    callback: TransformCallback
  ): void {
    this.gzip._transform(chunk, encoding, callback);
  }

  public _flush(callback: TransformCallback): void {
    this.gzip._flush(callback);
  }

  // public _final(callback: (error?: Error) => void): void {
  //   this.gzip._final(callback);
  // }

  // public _write(
  //   chunk: any,
  //   encoding: BufferEncoding,
  //   callback: (error?: Error) => void
  // ): void {
  //   this.gzip._write(chunk, encoding, callback);
  // }

  // public _read(size: number): void {
  //   this.gzip._read(size);
  // }

  // public _writev(
  //   chunks: { chunk: any; encoding: BufferEncoding }[],
  //   callback: (error?: Error) => void
  // ): void {
  //   this.gzip._writev(chunks, callback);
  // }

  // public resume(): this {
  //   super.resume();
  //   this.gzip.resume();
  //   return this;
  // }

  // public pause(): this {
  //   super.pause();
  //   this.gzip.pause();
  //   return this;
  // }

  // public destroy(error?: Error): this {
  //   super.destroy();
  //   this.gzip.destroy();
  //   return this;
  // }
}

describe('sitemap stream', () => {
  const sampleURLs = ['http://example.com', 'http://example.com/path'];

  it('pops out the preamble and closetag', async () => {
    const sms = new SitemapStream({
      xslUrl: 'https://example.com/style.xsl',
    });
    sms.write(sampleURLs[0]);
    sms.write(sampleURLs[1]);
    sms.end();
    expect(sms.itemCount).toBe(2);
    const outputStr = (await streamToPromise(sms)).toString();
    expect(sms.byteCount).toBe(outputStr.length);
    expect(outputStr).toBe(
      charset +
        '<?xml-stylesheet type="text/xsl" href="https://example.com/style.xsl"?>' +
        urlsetMins +
        `<url><loc>${sampleURLs[0]}/</loc></url>` +
        `<url><loc>${sampleURLs[1]}</loc></url>` +
        closetag
    );
  });

  it.only('gzip test', async () => {
    const drain = [];
    const sink = new Writable({
      write(chunk, enc, next): void {
        drain.push(chunk);
        next();
      },
    });

    const pipelineCallback = jest.fn().mockImplementation((error) => {
      console.log('callback saw error', error);
    });

    const garbage = createReadStream(
      path.join(__dirname, 'sitemap-stream.test.ts')
    );

    const gzip = new GzipSourceErrorSuppression();
    pipeline(garbage, gzip, sink, pipelineCallback);

    // Closing should generate a valid file after the exception
    const outputStr = gunzipSync(Buffer.concat(drain)).toString();

    expect(outputStr).toBe(
      preamble + `<url><loc>${sampleURLs[0]}/</loc></url>` + closetag
    );

    expect(pipelineCallback).toBeCalledTimes(1);
  });

  it('normal write closes cleanly', async () => {
    const drain = [];
    const sink = new Writable({
      write(chunk, enc, next): void {
        drain.push(chunk);
        next();
      },
    });

    const pipelineCallback = jest.fn();

    const sms = new SitemapStream({ countLimit: 1 });

    pipeline(sms, sink, pipelineCallback);

    // const writeAsync = (
    //   chunk: any,
    //   encoding?: BufferEncoding
    // ): Promise<boolean> => {
    //   return new Promise<boolean>((resolve, reject) => {
    //     const writeReturned = sms.write(chunk, encoding, (error) => {
    //       if (error !== undefined) {
    //         reject(error);
    //       } else {
    //         resolve(writeReturned);
    //       }
    //     });
    //   });
    // };

    // This write will succeed
    await sms.writeAsync(sampleURLs[0]);
    expect(sms.itemCount).toBe(1);
    expect(sms.byteCount).toBe(375);
    expect(sms.wroteCloseTag).toBe(false);

    // Close the stream and wait for the sink to close
    sms.end();

    expect(sms.wroteCloseTag).toBe(true);

    await finishedAsync(sink);

    // Write after error should indicate already closed
    await expect(async () => sms.writeAsync(sampleURLs[1])).rejects.toThrow(
      'write after end'
    );

    // Node 12 hangs on this await, Node 14 fixes it
    // if (process.version.split('.')[0] !== 'v12') {
    //   await finishedAsync(sms);
    // }

    // Closing should generate a valid file after the exception
    const outputStr = Buffer.concat(drain).toString();

    expect(sms.byteCount).toBe(outputStr.length);
    expect(outputStr).toBe(
      preamble + `<url><loc>${sampleURLs[0]}/</loc></url>` + closetag
    );

    expect(pipelineCallback).toBeCalledTimes(1);
  });

  it('normal write closes cleanly - gzip', async () => {
    const drain = [];
    const sink = new Writable({
      write(chunk, enc, next): void {
        drain.push(chunk);
        next();
      },
    });

    const pipelineCallback = jest.fn();

    const sms = new SitemapStream({ countLimit: 1 });

    pipeline(sms, createGzip(), sink, pipelineCallback);

    // const writeAsync = (
    //   chunk: any,
    //   encoding?: BufferEncoding
    // ): Promise<boolean> => {
    //   return new Promise<boolean>((resolve, reject) => {
    //     const writeReturned = sms.write(chunk, encoding, (error) => {
    //       if (error !== undefined) {
    //         reject(error);
    //       } else {
    //         resolve(writeReturned);
    //       }
    //     });
    //   });
    // };

    // This write will succeed
    await sms.writeAsync(sampleURLs[0]);
    expect(sms.itemCount).toBe(1);
    expect(sms.byteCount).toBe(375);
    expect(sms.wroteCloseTag).toBe(false);

    // Close the stream and wait for the sink to close
    sms.end();

    expect(sms.wroteCloseTag).toBe(true);

    await finishedAsync(sink);

    // Write after error should indicate already closed
    await expect(async () => sms.writeAsync(sampleURLs[1])).rejects.toThrow(
      'write after end'
    );

    // Node 12 hangs on this await, Node 14 fixes it
    // if (process.version.split('.')[0] !== 'v12') {
    //   await finishedAsync(sms);
    // }

    // Closing should generate a valid file after the exception
    const outputStr = gunzipSync(Buffer.concat(drain)).toString();

    expect(sms.byteCount).toBe(outputStr.length);
    expect(outputStr).toBe(
      preamble + `<url><loc>${sampleURLs[0]}/</loc></url>` + closetag
    );

    expect(pipelineCallback).toBeCalledTimes(1);
  });

  it('emits error on item count would be exceeded', async () => {
    const sms = new SitemapStream({ countLimit: 1 });
    const drain = [];
    const sink = new Writable({
      write(chunk, enc, next): void {
        drain.push(chunk);
        next();
      },
    });

    const pipelineCallback = jest.fn();

    pipeline(sms, sink, pipelineCallback);

    // This write will succeed
    await sms.writeAsync(sampleURLs[0]);
    expect(sms.itemCount).toBe(1);
    expect(sms.wroteCloseTag).toBe(false);

    // This write will fail
    await expect(async () => sms.writeAsync(sampleURLs[1])).rejects.toThrow(
      'Item count limit would be exceeded, not writing, stream will close'
    );

    // Node 12 hangs on this await, Node 14 fixes it
    if (process.version.split('.')[0] !== 'v12') {
      // This is the signal that the file was closed correctly
      expect(sms.wroteCloseTag).toBe(true);
      expect(sms.destroyed).toBe(true);

      await finishedAsync(sms);
    }

    // Note: we cannot use streamToPromise here because
    // it just hangs in this case - That's probably a problem to fix.
    // const outputStr = (await streamToPromise(sms)).toString();

    // Closing should generate a valid file with contents
    // from before the exception
    const outputStr = Buffer.concat(drain).toString();

    expect(outputStr).toBe(
      preamble + `<url><loc>${sampleURLs[0]}/</loc></url>` + closetag
    );
    expect(sms.byteCount).toBe(outputStr.length);

    expect(pipelineCallback).toBeCalledTimes(1);
  });

  it('throws on byte count would be exceeded', async () => {
    const drain = [];
    const sink = new Writable({
      write(chunk, enc, next): void {
        drain.push(chunk);
        next();
      },
    });

    const pipelineCallback = jest.fn();

    const sms = new SitemapStream({ byteLimit: 400 });

    pipeline(sms, sink, pipelineCallback);

    // This write will succeed
    await sms.writeAsync(sampleURLs[0]);
    expect(sms.itemCount).toBe(1);
    expect(sms.byteCount).toBe(375);
    expect(sms.wroteCloseTag).toBe(false);

    await expect(async () => sms.writeAsync(sampleURLs[1])).rejects.toThrow(
      new ByteLimitExceededError(
        'Byte count limit would be exceeded, not writing, stream will close'
      )
    );

    expect(sms.wroteCloseTag).toBe(true);
    expect(sms.destroyed).toBe(true);

    // Write after error should indicate already closed
    await expect(async () => sms.writeAsync(sampleURLs[1])).rejects.toThrow(
      'Cannot call write after a stream was destroyed'
    );

    try {
      await sms.writeAsync(sampleURLs[1]);
    } catch (error: any) {
      expect(error.code).toBe('ERR_STREAM_DESTROYED');
    }

    // Node 12 hangs on this await, Node 14 fixes it
    if (process.version.split('.')[0] !== 'v12') {
      await finishedAsync(sms);
    }

    // Closing should generate a valid file after the exception
    const outputStr = Buffer.concat(drain).toString();

    expect(sms.byteCount).toBe(outputStr.length);
    expect(outputStr).toBe(
      preamble + `<url><loc>${sampleURLs[0]}/</loc></url>` + closetag
    );

    expect(pipelineCallback).toBeCalledTimes(1);
  });

  it.only('throws on byte count would be exceeded - gzip', async () => {
    const drain = [];
    const sink = new Writable({
      write(chunk, enc, next): void {
        drain.push(chunk);
        next();
      },
    });
    // const errorSuppression = new SitemapStreamErrorSuppression();

    const pipelineCallback = jest.fn().mockImplementation((error) => {
      console.log('callback saw error', error);
    });

    const sms = new SitemapStream({ byteLimit: 400 });
    const smsError = sms.on('error', function handleSourceError(error) {
      console.log('sms.error:', error.message);
    });

    const gzip = new GzipSourceErrorSuppression();
    pipeline(smsError, gzip, sink, pipelineCallback);

    // This write will succeed
    await sms.writeAsync(sampleURLs[0]);
    expect(sms.itemCount).toBe(1);
    expect(sms.byteCount).toBe(375);
    expect(sms.wroteCloseTag).toBe(false);

    await expect(async () => sms.writeAsync(sampleURLs[1])).rejects.toThrow(
      new ByteLimitExceededError(
        'Byte count limit would be exceeded, not writing, stream will close'
      )
    );

    // End the gzip stream since it's been unpiped
    // gzip.end();

    expect(sms.wroteCloseTag).toBe(true);
    expect(sms.destroyed).toBe(true);

    // Write after error should indicate already closed
    // await expect(async () => sms.writeAsync(sampleURLs[1])).rejects.toThrow(
    //   'Cannot call write after a stream was destroyed'
    // );

    // try {
    //   await sms.writeAsync(sampleURLs[1]);
    // } catch (error: any) {
    //   expect(error.code).toBe('ERR_STREAM_WRITE_AFTER_END');
    // }

    // Node 12 hangs on this await, Node 14 fixes it
    if (process.version.split('.')[0] !== 'v12') {
      await finishedAsync(sink);
    }

    // Closing should generate a valid file after the exception
    const outputStr = gunzipSync(Buffer.concat(drain)).toString();

    expect(sms.byteCount).toBe(outputStr.length);
    expect(outputStr).toBe(
      preamble + `<url><loc>${sampleURLs[0]}/</loc></url>` + closetag
    );

    expect(pipelineCallback).toBeCalledTimes(1);
  });

  it('throws on item count would be exceeded', async () => {
    const drain = [];
    const sink = new Writable({
      write(chunk, enc, next): void {
        drain.push(chunk);
        next();
      },
    });

    const pipelineCallback = jest.fn();

    const sms = new SitemapStream({ countLimit: 1 });

    pipeline(sms, sink, pipelineCallback);

    // This write will succeed
    await sms.writeAsync(sampleURLs[0]);
    expect(sms.itemCount).toBe(1);
    expect(sms.byteCount).toBe(375);
    expect(sms.wroteCloseTag).toBe(false);

    await expect(async () => sms.writeAsync(sampleURLs[1])).rejects.toThrow(
      new CountLimitExceededError(
        'Item count limit would be exceeded, not writing, stream will close'
      )
    );

    expect(sms.wroteCloseTag).toBe(true);
    expect(sms.destroyed).toBe(true);

    // Write after error should indicate already closed
    await expect(async () => sms.writeAsync(sampleURLs[1])).rejects.toThrow(
      'Cannot call write after a stream was destroyed'
    );

    // Node 12 hangs on this await, Node 14 fixes it
    if (process.version.split('.')[0] !== 'v12') {
      await finishedAsync(sms);
    }

    // Closing should generate a valid file after the exception
    const outputStr = Buffer.concat(drain).toString();

    expect(sms.byteCount).toBe(outputStr.length);
    expect(outputStr).toBe(
      preamble + `<url><loc>${sampleURLs[0]}/</loc></url>` + closetag
    );

    expect(pipelineCallback).toBeCalledTimes(1);
  });

  it('pops out custom xmlns', async () => {
    const sms = new SitemapStream({
      xmlns: {
        news: false,
        video: true,
        image: true,
        xhtml: true,
        custom: [
          'xmlns:custom="http://example.com"',
          'xmlns:example="http://o.example.com"',
        ],
      },
    });
    sms.write(sampleURLs[0]);
    sms.write(sampleURLs[1]);
    sms.end();
    expect((await streamToPromise(sms)).toString()).toBe(
      minimumns +
        xhtml +
        image +
        video +
        ' xmlns:custom="http://example.com" xmlns:example="http://o.example.com"' +
        '>' +
        `<url><loc>${sampleURLs[0]}/</loc></url>` +
        `<url><loc>${sampleURLs[1]}</loc></url>` +
        closetag
    );
  });

  it('normalizes passed in urls', async () => {
    const source = ['/', '/path'];
    const sms = new SitemapStream({ hostname: 'https://example.com/' });
    sms.write(source[0]);
    sms.write(source[1]);
    sms.end();
    expect((await streamToPromise(sms)).toString()).toBe(
      preamble +
        `<url><loc>https://example.com/</loc></url>` +
        `<url><loc>https://example.com/path</loc></url>` +
        closetag
    );
  });

  it('invokes custom errorHandler', async () => {
    const source = [
      { url: '/', changefreq: 'daily' },
      { url: '/path', changefreq: 'invalid' },
    ];
    const errorHandlerMock = jest.fn();
    const sms = new SitemapStream({
      hostname: 'https://example.com/',
      errorHandler: errorHandlerMock,
    });
    sms.write(source[0]);
    sms.write(source[1]);
    sms.end();
    await new Promise((resolve) => sms.on('finish', resolve));
    expect(errorHandlerMock.mock.calls.length).toBe(1);
    expect((await streamToPromise(sms)).toString()).toBe(
      preamble +
        `<url><loc>https://example.com/</loc><changefreq>daily</changefreq></url>` +
        `<url><loc>https://example.com/path</loc><changefreq>invalid</changefreq></url>` +
        closetag
    );
  });

  describe('countLimit / byteLimit properties', () => {
    it('exposes countLimit property', () => {
      const sms = new SitemapStream({ countLimit: 400 });
      expect(sms.countLimit).toBe(400);
    });

    it('allows setting countLimit property if unset', () => {
      const sms = new SitemapStream();
      expect(sms.countLimit).toBeUndefined();
      sms.countLimit = 400;
      expect(sms.countLimit).toBe(400);
    });

    it('throws if countLimit set after items written', async () => {
      const sink = createWriteStream('/dev/null');
      const sms = new SitemapStream({});
      sms.pipe(sink);

      // This write will succeed
      await sms.writeAsync(sampleURLs[0]);
      expect(sms.itemCount).toBe(1);

      expect(() => {
        sms.countLimit = 2;
      }).toThrow('cannot set countLimit if items written already');

      sms.end();
      await finishedAsync(sink);
    });

    it('throws if countLimit set twice', async () => {
      const sms = new SitemapStream({});
      sms.countLimit = 1;

      expect(() => {
        sms.countLimit = 2;
      }).toThrow('cannot set countLimit if already set');

      await sms.writeAsync(sampleURLs[0]);
      sms.end();
    });

    it('exposes byteLimit property', () => {
      const sms = new SitemapStream({ byteLimit: 400 });
      expect(sms.byteLimit).toBe(400);
    });

    it('allows setting byteLimit property if unset', () => {
      const sms = new SitemapStream();
      expect(sms.byteLimit).toBeUndefined();
      sms.byteLimit = 400;
      expect(sms.byteLimit).toBe(400);
    });

    it('throws if byteLimit set after items written', async () => {
      const sink = createWriteStream('/dev/null');
      const sms = new SitemapStream({});
      sms.pipe(sink);

      // This write will succeed
      await sms.writeAsync(sampleURLs[0]);
      expect(sms.itemCount).toBe(1);

      expect(() => {
        sms.byteLimit = 2;
      }).toThrow('cannot set byteLimit if items written already');

      sms.end();
      await finishedAsync(sink);
    });

    it('throws if byteLimit set twice', async () => {
      const sms = new SitemapStream({});
      sms.byteLimit = 1000;

      expect(() => {
        sms.byteLimit = 2000;
      }).toThrow('cannot set byteLimit if already set');

      await sms.writeAsync(sampleURLs[0]);
      sms.end();
    });
  });
});
