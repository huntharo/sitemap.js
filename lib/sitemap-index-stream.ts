import {
  Transform,
  TransformOptions,
  TransformCallback,
  finished,
} from 'stream';
import { promisify } from 'util';
import { IndexItem, SitemapItemLoose, ErrorLevel } from './types';
import { SitemapStream, stylesheetInclude } from './sitemap-stream';
import { element, otag, ctag } from './sitemap-xml';
import { WriteStream } from 'fs';
import {
  ByteLimitExceededError,
  CountLimitExceededError,
  WriteAfterCloseTagError,
} from './errors';

const finishedAsync = promisify(finished);

export enum IndexTagNames {
  sitemap = 'sitemap',
  loc = 'loc',
  lastmod = 'lastmod',
}

const xmlDec = '<?xml version="1.0" encoding="UTF-8"?>';

const sitemapIndexTagStart =
  '<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">';
const closetag = '</sitemapindex>';

export interface SitemapIndexStreamOptions extends TransformOptions {
  lastmodDateOnly?: boolean;
  level?: ErrorLevel;
  xslUrl?: string;
}
const defaultStreamOpts: SitemapIndexStreamOptions = {};
export class SitemapIndexStream extends Transform {
  lastmodDateOnly: boolean;
  level: ErrorLevel;
  xslUrl?: string;
  private hasHeadOutput: boolean;
  constructor(opts = defaultStreamOpts) {
    opts.objectMode = true;
    super(opts);
    this.hasHeadOutput = false;
    this.lastmodDateOnly = opts.lastmodDateOnly || false;
    this.level = opts.level ?? ErrorLevel.WARN;
    this.xslUrl = opts.xslUrl;
  }

  _transform(
    item: IndexItem | string,
    encoding: string,
    callback: TransformCallback
  ): void {
    if (!this.hasHeadOutput) {
      this.hasHeadOutput = true;
      let stylesheet = '';
      if (this.xslUrl) {
        stylesheet = stylesheetInclude(this.xslUrl);
      }
      this.push(xmlDec + stylesheet + sitemapIndexTagStart);
    }
    this.push(otag(IndexTagNames.sitemap));
    if (typeof item === 'string') {
      this.push(element(IndexTagNames.loc, item));
    } else {
      this.push(element(IndexTagNames.loc, item.url));
      if (item.lastmod) {
        const lastmod: string = new Date(item.lastmod).toISOString();
        this.push(
          element(
            IndexTagNames.lastmod,
            this.lastmodDateOnly ? lastmod.slice(0, 10) : lastmod
          )
        );
      }
    }
    this.push(ctag(IndexTagNames.sitemap));
    callback();
  }

  _flush(cb: TransformCallback): void {
    this.push(closetag);
    cb();
  }

  /**
   * Async helper for writing items to the stream
   *
   * @param chunk SitemapItemLoose | URL string
   * @param encoding
   * @returns
   */
  public async writeAsync(
    chunk: IndexItem | string,
    encoding?: BufferEncoding
  ): Promise<boolean> {
    return new Promise<boolean>((resolve, reject) => {
      const writeReturned = this.write(chunk, encoding, (error) => {
        if (error !== undefined) {
          reject(error);
        } else {
          resolve(writeReturned);
        }
      });
    });
  }
}

type getSitemapStream = (
  i: number
  // countLimit: number,
  // byteLimit: number
) => [IndexItem | string, SitemapStream, WriteStream];

export interface SitemapAndIndexStreamOptions
  extends SitemapIndexStreamOptions {
  level?: ErrorLevel;

  /**
   * @deprecated Use `countLimit` instead, `limit` will overwrite `countLimit` if specified.
   */
  limit?: number;

  /**
   * Byte limit to allow in each sitemap before rotating to a new sitemap
   *
   * Sitemaps are supposed to be 50 MB or less in total size
   *
   * @default 45MB (45 * 1024 * 1024 bytes)
   */
  byteLimit?: number;

  /**
   * Count of items to allow in each sitemap before rotating to a new sitemap
   *
   * Sitemaps are supposed to have 50,000 or less items
   *
   * @default 45,000
   */
  countLimit?: number;

  /**
   * Called every time a sitemap file needs to be created either
   * due to initialization or due to exceeding `byteLimit` or `countLimit`.
   *
   * Returns an array of:
   *  - 0: string or IndexItem with the URL where the newly
   *       created SitemapStream is intended to be hosted
   *  - 1: SitemapStream destination for writing SitemapItem's
   *  - 2: WriteStream for the underlying file or final sink
   *       for the written items.
   *       Used to wait for completion of writing to the sink.
   */
  getSitemapStream: getSitemapStream;
}
export class SitemapAndIndexStream extends SitemapIndexStream {
  private itemCountTotal: number;
  private sitemapCount: number;
  private getSitemapStream: getSitemapStream;
  private currentSitemap: SitemapStream;
  private currentSitemapPipeline?: WriteStream;
  private idxItem: IndexItem | string;
  private countLimit: number;
  private byteLimit: number;
  private pendingWrites: number;
  private pendingFlushCallbacks: TransformCallback[];
  /**
   * Create a sitemap index and set of sitemaps from a stream
   * of sitemap items.
   *
   * The number of sitemaps is determined by `byteLimit` and `countLimit`,
   * with new sitemaps being created either exactly at the `countLimt`
   * or when writing an item would cause the `byteLimit` to be exceeded.
   *
   * @param opts Options
   */
  constructor(opts: SitemapAndIndexStreamOptions) {
    opts.objectMode = true;
    super(opts);
    this.itemCountTotal = 0;
    this.pendingWrites = 0;
    this.pendingFlushCallbacks = [];
    this.sitemapCount = 0;
    this.getSitemapStream = opts.getSitemapStream;
    [this.idxItem, this.currentSitemap, this.currentSitemapPipeline] =
      this.getSitemapStream(this.sitemapCount);

    this.currentSitemap.on('error', (error: any) => {
      if (
        !(
          error instanceof ByteLimitExceededError ||
          error instanceof CountLimitExceededError ||
          error instanceof WriteAfterCloseTagError ||
          error.code === 'ERR_STREAM_WRITE_AFTER_END' ||
          error.code === 'ERR_STREAM_DESTROYED'
        )
      ) {
        throw error;
      }
    });
    this.countLimit = opts.limit ?? opts.countLimit ?? 45000;
    this.byteLimit = opts.byteLimit ?? 45 * 1024 * 1024;
    this.currentSitemap.countLimit = this.countLimit;
    this.currentSitemap.byteLimit = this.byteLimit;
  }

  // public _writeSMI(
  //   item: SitemapItemLoose,
  //   encoding: string,
  //   callback: () => void
  // ): void {
  //   if (
  //     !this.currentSitemap.write(item, (error: any) => {
  //       if (error !== undefined && error !== null) {
  //         if (
  //           error instanceof ByteLimitExceededError ||
  //           error instanceof CountLimitExceededError
  //         ) {
  //           // Handle the rotate
  //           this.sitemapCount++;

  //           // Item could not be written because sitemap would overflow
  //           // Create a new sitemap and write the item to the new sitemap
  //           [this.idxItem, this.currentSitemap, this.currentSitemapPipeline] =
  //             this.getSitemapStream(this.sitemapCount);
  //           this.currentSitemap.byteLimit = this.byteLimit;
  //           this.currentSitemap.countLimit = this.countLimit;
  //           this.currentSitemap.on('error', (error: any) => {
  //             if (
  //               !(
  //                 error instanceof ByteLimitExceededError ||
  //                 error instanceof CountLimitExceededError ||
  //                 error instanceof WriteAfterCloseTagError ||
  //                 error.code === 'ERR_STREAM_WRITE_AFTER_END' ||
  //                 error.code === 'ERR_STREAM_DESTROYED'
  //               )
  //             ) {
  //               throw error;
  //             }
  //           });

  //           if (
  //             this.currentSitemapPipeline !== undefined &&
  //             !this.currentSitemapPipeline.writableFinished
  //           ) {
  //             this.currentSitemapPipeline.on('finish', () =>
  //               this._writeSMI(item, encoding, () => {
  //                 // push to index stream
  //                 super._transform(this.idxItem, encoding, callback);
  //               })
  //             );
  //           } else {
  //             this._writeSMI(item, encoding, () => {
  //               // push to index stream
  //               super._transform(this.idxItem, encoding, callback);
  //             });
  //           }

  //           return true;
  //         }

  //         if (
  //           error instanceof WriteAfterCloseTagError ||
  //           error.code === 'ERR_STREAM_WRITE_AFTER_END' ||
  //           error.code === 'ERR_STREAM_DESTROYED'
  //         ) {
  //           // Write the item again to the current sitemap
  //           // This will only happen once per item
  //           this._writeSMI(item, encoding, callback);

  //           return true;
  //         }

  //         return false;
  //       }
  //     })
  //   ) {
  //     this.currentSitemap.once('drain', callback);
  //   } else {
  //     process.nextTick(callback);
  //   }
  // }

  public _transform(
    item: SitemapItemLoose,
    encoding: string,
    callback: TransformCallback
  ): void {
    this.pendingWrites++;

    const superTransformAsync = async (
      item: string | IndexItem,
      encoding: string
    ): Promise<void> => {
      return new Promise<void>((resolve, reject) => {
        super._transform(item, encoding, (error) => {
          if (error !== undefined && error !== null) {
            reject(error);
          } else {
            resolve();
          }
        });
      });
    };

    (async () => {
      try {
        const sitemapAtStart = this.currentSitemap;
        this.itemCountTotal++;

        if (typeof item !== 'string') {
          console.log('not a string');
        }

        if (this.itemCountTotal === undefined) {
          console.log('should not happen');
        }

        // eslint-disable-next-line no-constant-condition
        while (true) {
          try {
            if (this.itemCountTotal === 1) {
              this.sitemapCount++;

              await this.currentSitemap.writeAsync(
                item,
                encoding as BufferEncoding
              );
              await superTransformAsync(this.idxItem, encoding);
            } else {
              await this.currentSitemap.writeAsync(
                item,
                encoding as BufferEncoding
              );
            }

            console.log(`Wrote item to sitemap`, {
              pendingFlushCallbacks: this.pendingFlushCallbacks.length,
              pendingWrites: this.pendingWrites,
              sitemapCount: this.sitemapCount,
              itemCountTotal: this.itemCountTotal,
              item,
              sitemapPath: this.currentSitemapPipeline?.path,
            });

            // If we get here it was a success
            callback();
            return;
          } catch (error: any) {
            // Check if we need to rotate the file
            if (
              error instanceof ByteLimitExceededError ||
              error instanceof CountLimitExceededError
            ) {
              console.log(`Rotating sitemap`, {
                pendingFlushCallbacks: this.pendingFlushCallbacks.length,
                pendingWrites: this.pendingWrites,
                sitemapCount: this.sitemapCount,
                itemCountTotal: this.itemCountTotal,
                item,
                sitemapPath: this.currentSitemapPipeline?.path,
              });

              if (!this.currentSitemap.destroyed) {
                this.currentSitemap.end();
                await finishedAsync(this.currentSitemap);
              }

              console.log(`Rotating sitemap - prior finished`, {
                pendingFlushCallbacks: this.pendingFlushCallbacks.length,
                pendingWrites: this.pendingWrites,
                sitemapCount: this.sitemapCount,
                itemCountTotal: this.itemCountTotal,
                item,
                sitemapPath: this.currentSitemapPipeline?.path,
              });

              // Item could not be written because sitemap would overflow
              // Create a new sitemap and write the item to the new sitemap
              [this.idxItem, this.currentSitemap, this.currentSitemapPipeline] =
                this.getSitemapStream(this.sitemapCount);

              // Set the limits on the new sitemap
              this.currentSitemap.byteLimit = this.byteLimit;
              this.currentSitemap.countLimit = this.countLimit;

              // This is to prevent the default .on('error') handler
              // from throwing on every exception, even those that we handle
              this.currentSitemap.on('error', (error: any) => {
                if (
                  !(
                    error instanceof ByteLimitExceededError ||
                    error instanceof CountLimitExceededError ||
                    error instanceof WriteAfterCloseTagError ||
                    error.code === 'ERR_STREAM_WRITE_AFTER_END' ||
                    error.code === 'ERR_STREAM_DESTROYED'
                  )
                ) {
                  throw error;
                }
              });

              // console.log(`Rotating sitemap - calling super._transform`, {
              //   pendingFlushCallbacks: this.pendingFlushCallbacks.length,
              //   pendingWrites: this.pendingWrites,
              //   sitemapCount: this.sitemapCount,
              //   itemCountTotal: this.itemCountTotal,
              //   item,
              //   sitemapPath: this.currentSitemapPipeline?.path,
              // });

              await superTransformAsync(this.idxItem, encoding);

              // console.log(`Rotating sitemap - super._transform returned`, {
              //   pendingFlushCallbacks: this.pendingFlushCallbacks.length,
              //   pendingWrites: this.pendingWrites,
              //   sitemapCount: this.sitemapCount,
              //   itemCountTotal: this.itemCountTotal,
              //   item,
              //   sitemapPath: this.currentSitemapPipeline?.path,
              // });

              // Handle the rotate
              this.sitemapCount++;

              // Loop around to try the write again
              continue;
            } else if (
              error instanceof WriteAfterCloseTagError ||
              error.code === 'ERR_STREAM_WRITE_AFTER_END' ||
              error.code === 'ERR_STREAM_DESTROYED'
            ) {
              console.log(`Other exception`, {
                errorName: error.name,
                errorCode: error.code,
                errorMessage: error.message,
                pendingFlushCallbacks: this.pendingFlushCallbacks.length,
                pendingWrites: this.pendingWrites,
                sitemapCount: this.sitemapCount,
                itemCountTotal: this.itemCountTotal,
                item,
                sitemapPath: this.currentSitemapPipeline?.path,
              });

              if (this.currentSitemap === sitemapAtStart) {
                callback(new TypeError('resubmit to same sitemap not allowed'));
                return;
              } else {
                // Re-submit the write to the current sitemap file
                continue;
              }
            }

            callback(error);
            return;
          }
        }
      } catch (error: any) {
        console.log(`Error from catch`, {
          errorName: error.name,
          errorCode: error.code,
          errorMessage: error.message,
          pendingFlushCallbacks: this.pendingFlushCallbacks.length,
          pendingWrites: this.pendingWrites,
          sitemapCount: this.sitemapCount,
          itemCountTotal: this.itemCountTotal,
          item,
          sitemapPath: this.currentSitemapPipeline?.path,
        });
      } finally {
        this.pendingWrites--;

        if (this.pendingWrites === 0 && this.pendingFlushCallbacks.length > 0) {
          this.doTheFlush();
        }
      }
    })();
  }

  _flush(cb: TransformCallback): void {
    console.log('_flush called', {
      pendingFlushCallbacks: this.pendingFlushCallbacks.length,
      pendingWrites: this.pendingWrites,
      sitemapCount: this.sitemapCount,
      itemCountTotal: this.itemCountTotal,
      sitemapPath: this.currentSitemapPipeline?.path,
    });
    if (this.pendingWrites > 0 || this.pendingFlushCallbacks.length > 0) {
      this.pendingFlushCallbacks.push(cb);
    } else {
      this.pendingFlushCallbacks.push(cb);
      this.doTheFlush();
    }
  }

  private doTheFlush(): void {
    try {
      console.log(`doTheFlush`, {
        pendingFlushCallbacks: this.pendingFlushCallbacks.length,
        pendingWrites: this.pendingWrites,
        sitemapCount: this.sitemapCount,
        itemCountTotal: this.itemCountTotal,
        sitemapPath: this.currentSitemapPipeline?.path,
      });

      const callbacks = this.pendingFlushCallbacks.slice();
      this.pendingFlushCallbacks = [];
      const onFinish = () => {
        callbacks.forEach((cb) => {
          console.log(`doTheFlush - calling super._flush`, {
            pendingFlushCallbacks: this.pendingFlushCallbacks.length,
            pendingWrites: this.pendingWrites,
            sitemapCount: this.sitemapCount,
            itemCountTotal: this.itemCountTotal,
            sitemapPath: this.currentSitemapPipeline?.path,
          });
          this._flush(cb);
          console.log(`doTheFlush - returned from super._flush`, {
            pendingFlushCallbacks: this.pendingFlushCallbacks.length,
            pendingWrites: this.pendingWrites,
            sitemapCount: this.sitemapCount,
            itemCountTotal: this.itemCountTotal,
            sitemapPath: this.currentSitemapPipeline?.path,
          });
        });
      };
      this.currentSitemapPipeline?.on('finish', onFinish);
      this.currentSitemap.end(
        !this.currentSitemapPipeline ? onFinish : undefined
      );
    } catch (error: any) {
      console.log(`doTheFlush Error`, {
        errorName: error.name,
        errorCode: error.code,
        errorMessage: error.message,
        pendingFlushCallbacks: this.pendingFlushCallbacks.length,
        pendingWrites: this.pendingWrites,
        sitemapCount: this.sitemapCount,
        itemCountTotal: this.itemCountTotal,
        sitemapPath: this.currentSitemapPipeline?.path,
      });
    }
  }
}
