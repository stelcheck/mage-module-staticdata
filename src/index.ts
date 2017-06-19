import 'reflect-metadata'

import * as validator from 'class-validator'
import * as fs from 'fs'
import * as mage from 'mage'
import * as zlib from 'zlib'

/**
 * We internally track three data types:
 *
 *   1. Scalar, which are dropped in place just like any other data
 *   2. Array, for which we will instanciate one instance of each
 *   3. Object, for which we instanciate an instance
 *
 * These types are used to parse and stringify all attributes
 * and nested attributes of the static data object.
 *
 * @export
 * @enum {number}
 */
export const enum ValueType {
  Scalar,
  Array,
  Object
}

/**
 * Target class instance.
 *
 * @export
 * @interface ITarget
 */
export interface ITarget {
  [key: string]: any
}

/**
 * Metadata we will stack on our target classes
 *
 * @export
 * @interface IMetaData
 */
export interface IMetaData {
  remoteName: string
  opts: any
  type: ValueType
}

export interface IMetaDataMap {
  [key: string]: IMetaData
}

/**
 * We specifically mention the type of the hidden
 * static metadata container
 *
 * @export
 * @interface StaticDataClass
 * @extends {ITarget}
 */
export class StaticDataClass implements ITarget {
  public static _staticDataMeta: IMetaDataMap
}

/**
 * Compute the metadata which we will attach
 * to a static data class
 *
 * @param {*} target
 * @param {string} key
 * @param {string} remoteName
 * @param {*} [childClass]
 * @returns
 */
function computeMetadata(target: any, key: string, remoteName: string, opts?: any) {
  const Info = Reflect.getMetadata('design:type', target, key)
  const instance = new Info()
  let type = ValueType.Object

  if (instance instanceof String || instance instanceof Number || instance instanceof Boolean) {
    type = ValueType.Scalar
  } else if (Array.isArray(instance)) {
    type = ValueType.Array
  }

  return {
    remoteName,
    opts,
    type
  }
}

/**
 * Return the metadata tree for this static data class
 *
 * @param target
 */
function extractMetaData(target: typeof StaticDataClass) {
  const meta = target._staticDataMeta
  const keys = Object.keys(meta)
  const ret: any = {}

  for (const key of keys) {
    const {
      opts,
      remoteName: name,
      type
    } = meta[key]

    ret[key] = {
      name,
      type,
      meta: opts && opts._staticDataMeta ? extractMetaData(opts) : opts
    }
  }

  return ret
}

/**
 * The walk function processes anonymous data
 * against the schema defined by the class metadata
 *
 * @param {*} data
 * @param {StaticDataClass} Target
 * @returns
 */
function walk(data: any, Target: typeof StaticDataClass) {
  const instance = new (<any> Target)()
  const meta = Target._staticDataMeta
  const keys = Object.keys(meta)

  if (!meta) {
    throw new Error('Missing metadata!')
  }

  for (const key of keys) {
    const keyMeta = meta[key]
    const val = data[key]

    instance[key] = parseAttribute(val, keyMeta)
  }

  return instance
}

/**
 * Parse a single attribute based on received metadata
 *
 * @param {*} val
 * @param {any} keyMeta
 * @returns
 */
async function parseAttribute(val: any, keyMeta: IMetaData) {
   switch (keyMeta.type) {
    case ValueType.Scalar:
      return val

    case ValueType.Array:
      const arrayInstance: any[] = []
      for (const arrayVal of val) {
        const childInstance = walk(arrayVal, keyMeta.opts)
        await validator.validate(childInstance)
        arrayInstance.push(childInstance)
      }
      return arrayInstance

    case ValueType.Object:
      const instance = walk(val, keyMeta.opts)
      await validator.validate(instance)
      return instance
  }
}

/**
 * Static Data marker attribute
 *
 * This decorator helps us figure out:
 *
 *   1. What name will we use externally to refer to this attribute
 *      (e.g. what name we'll be using in our Excel files, etc)
 *   2. What the subtype of the data will be
 *
 * @export
 * @param {string} remoteName
 * @param {*} [childClass]
 * @returns
 */
export function StaticData(remoteName: string, childClass?: any) {
  return function (target: StaticDataClass, key: string) {
    const type: any = target.constructor
    const meta = computeMetadata(target, key, remoteName, childClass)

    if (!type._staticDataMeta) {
      type._staticDataMeta = {}
    }

    type._staticDataMeta[key] = meta
  }
}

/**
 * The abstractStaticDataModule class is to be used to
 * define a StaticDataModule class which will then immediately be
 * instanciated as a module.
 *
 * @export
 * @abstract
 * @class AbstractStaticDataModule
 */
export abstract class AbstractStaticDataModule {
  public abstract StaticDataClass: typeof StaticDataClass
  public abstract staticData: StaticDataClass

  private _dumpLocation: string

  constructor() {
    this._dumpLocation = mage.core.config.get('static.location') || 'static.dat'
  }

  /**
   * Inheriting class tells us how to access
   * the data (from database, from S3, etc)
   *
   * If an empty string is returned, we will try to
   * load a local dump during development; if no local
   * dumps are available, we will simply not load any data
   * in memory (this is useful for initially creating the
   * schema, then exporting it to a Google Spreadsheet)
   *
   * @abstract
   * @returns {Promise<string>} Stringified static data
   *
   * @memberof AbstractStaticDataModule
   */
  public async load(_state: mage.core.IState): Promise<string> {
    return this.loadDump()
  }

  /**
   * We send stringified data back to the inheriting class;
   * it is then responsible to store it back where it will read
   * it from upon startup or reload
   *
   * @abstract
   * @param {string} data
   * @returns {Promise<void>}
   *
   * @memberof AbstractStaticDataModule
   */
  public async store(_state: mage.core.IState, data: string): Promise<void> {
    return this.dump(data)
  }

  /**
   * MAGE module setup method
   *
   * @param {mage.core.IState} state
   * @param {(error?: Error) => void} callback
   *
   * @memberof AbstractStaticDataModule
   */
  public async setup(state: mage.core.IState, callback: (error?: Error) => void) {
    let data: string

    try {
      data = await this.load(state)
      this.staticData = this.parse(data)
    } catch (error) {
      // Todo: log error
      this.staticData = new this.StaticDataClass()
    }

    callback()
  }

  /**
   * Import data from a remote source (normally a Google Spreadsheet using
   * the MAGE Static Data Manager)
   *
   * @param {mage.core.IState} state
   * @param {*} data
   * @returns {Promise<void>}
   *
   * @memberof AbstractStaticDataModule
   */
  public async import(state: mage.core.IState, data: any): Promise<void> {
    await this.validate(state, data)
    await this.store(state, this.stringify(data))
    await this.load(state)
  }

  /**
   * Validate data from a remote source (normally a Google Spreadsheet using
   * the MAGE Static Data Manager)
   *
   * @param {mage.core.IState} state
   * @param {*} data
   * @returns {Promise<void>}
   *
   * @memberof AbstractStaticDataModule
   */
  public async validate(_state: mage.core.IState, data: any): Promise<void> {
    this.parse(data) // Will throw if invalid
  }

  /**
   * Export data to a remote destination (normally a Google Spreadsheet using
   * the MAGE Static Data Manager)
   *
   * @param {mage.core.IState} state
   * @returns {Promise<void>}
   *
   * @memberof AbstractStaticDataModule
   */
  public async export(_state: mage.core.IState): Promise<any> {
    return {
      data: this.staticData,
      schema: extractMetaData(this.StaticDataClass)
    }
  }

  /**
   * Store data to a local dump file
   *
   * @param {string} data
   * @returns {Promise<void>}
   *
   * @memberof AbstractStaticDataModule
   */
  public async dump(data: string = this.stringify()): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      zlib.deflate(data, (compressError, compressedData) => {
        if (compressError) {
          return reject(compressError)
        }

        fs.writeFile(this._dumpLocation, compressedData, (fileError) => {
          if (fileError) {
            return reject(fileError)
          }

          return resolve()
        })
      })
    })
  }

  /**
   * Load data from a local dump file
   *
   * @returns {Promise<string>}
   *
   * @memberof AbstractStaticDataModule
   */
  public async loadDump(): Promise<string> {
     return new Promise<string>((resolve, reject) => {
      fs.readFile(this._dumpLocation, (fileError, compressedData) => {
        if (fileError) {
          return reject(fileError)
        }

        zlib.inflate(compressedData, (decompressError, data) => {
          if (decompressError) {
            return reject(decompressError)
          }

          return resolve(data.toString())
        })
      })
    })
  }

  /**
   * Parse received data against the StaticDataClass
   *
   * @private
   * @param {string} json
   * @returns
   *
   * @memberof AbstractStaticDataModule
   */
  private parse(json: string) {
    const { StaticDataClass } = this
    const data: any = JSON.parse(json)

    return walk(data, StaticDataClass)
  }

  /**
   * Stringify the static data currently in memory
   *
   * @private
   * @returns
   *
   * @memberof AbstractStaticDataModule
   */
  private stringify(data?: any): string {
    return JSON.stringify(data || this.staticData)
  }
}
