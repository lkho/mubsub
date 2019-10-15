const EventEmitter = require('events').EventEmitter;

class Channel extends EventEmitter {
  /**
   * Channel constructor.
   *
   * @param {Connection} connection
   * @param {String} [name] optional channel/collection name, default is 'mubsub'
   * @param {Object} [options] optional options
   *   - `size` max size of the collection in bytes, default is 5mb
   *   - `max` max amount of documents in the collection
   *   - `retryInterval` time in ms to wait if no docs found, default is 200ms
   *   - `recreate` recreate the tailable cursor on error, default is true
   * @api public
   */
  constructor (connection, name, options) {
    super();
    this.setMaxListeners(0);

    options || (options = {});
    options.capped = true;
    options.size || (options.size = 1024 * 1024 * 5);
    options.strict = false;
    options.recreate = options.recreate == null ? true : options.recreate;
    options.retryInterval = options.retryInterval == null ? 200 : options.retryInterval;

    this.options = options;
    this.connection = connection;
    this.name = name || 'mubsub';
  }

  get listening () {
    return this.cursor;
  }

  /**
   * Create a channel collection.
   *
   * @return {Promise<void>}
   * @api private
   */
  async createCollection () {
    return new Promise((resolve, reject) => {
      const create = () => {
        this.connection.db.createCollection(
            this.name,
            this.options,
            (err, collection) => {
              if (err && err.message === 'collection already exists') {
                // pass
              } else if (err) {
                this.emit('error', err);
                return reject(err);
              }
              this.emit('collection', this.collection = collection);
              return resolve(collection);
            },
        );
      };
      this.connection.db ? create() : this.connection.once('connect', create);
    });
  };

  /**
   * Stop listening and close cursor.
   *
   * @return {Promise<void>} this
   * @api public
   */
  async close () {
    if (this.cursor) {
      const c = this.cursor;
      delete this.cursor;
      await c.close();
    }
  }

  /**
   * Publish an event.
   *
   * @param {String} event
   * @param {*} [message]
   * @return {Promise<Document>} the inserted document
   * @api public
   */
  async publish (event, message) {
    return this.createCollection().then(collection => {
      return new Promise((resolve, reject) => {
        collection.insertOne({ event, message }, { safe: true }, (err, docs) => {
          if (err) {
            reject(err);
          } else {
            resolve(docs.ops[0]);
          }
        });
      });
    });
  };

  /**
   * Subscribe an event.
   *
   * @param {String} [event] if no event passed - all events are subscribed.
   * @param {Function} listener
   * @return {Object<{unsubscribe: Function}>} unsubscribe function
   * @api public
   */
  subscribe (event, listener) {
    if (typeof event == 'function') {
      listener = event;
      event = 'message';
    }
    this.on(event, listener);
    return {
      unsubscribe: () => {
        this.removeListener(event, listener);
      },
    };
  };

  /**
   * Start listening for new documents
   *
   * @param {Object} [latest] latest document to start listening from
   * @return {Promise<void>}
   * @api private
   */
  async listen (latest) {
    if (this.listening) {
      return;
    }
    const r = await this.latest(latest);
    const collection = r.collection;

    const cursor = collection.find(
        { _id: { $gt: latest._id } },
        {
          tailable: true,
          awaitData: true,
          timeout: false,
          sortValue: { $natural: -1 },
          numberOfRetries: Number.MAX_VALUE,
          tailableRetryInterval: this.options.retryInterval,
        },
    );

    const next = doc => {
      // There is no document only if the cursor is closed by accident.
      // F.e. if collection was dropped or connection died.
      if (!doc) {
        this.emit('error', new Error('Mubsub: broken cursor.'));
        if (this.options.recreate) {
          setTimeout(() => {
            if (this.listening) {
              this.close();
              this.listen(latest);
            }
          }, this.options.retryInterval);
        }
        return;
      }

      latest = doc;

      if (doc.event) {
        this.emit(doc.event, doc.message);
        this.emit('message', doc.message);
      }
      this.emit('document', doc);
      process.nextTick(() => {
        cursor.next(next);
      });
    };

    process.nextTick(() => {
      cursor.next(next);
    });
    this.cursor = cursor;
  };

  /**
   * Get the latest document from the collection. Insert a dummy object in case
   * the collection is empty, because otherwise we don't get a tailable cursor
   * and need to poll in a loop.
   *
   * @param {Object} [latest] latest known document
   * @return {Promise<{latest: Document, collection: Collection}>}
   * @api private
   */
  async latest (latest) {
    return this.createCollection().then(collection => {
      return new Promise((resolve, reject) => {
        collection
            .find(latest ? { _id: latest._id } : null, { timeout: false })
            .sort({ $natural: -1 })
            .limit(1)
            .next((err, doc) => {
              if (err) return reject(err);
              if (doc) return resolve({ latest: doc, collection });
              collection.insertOne({ 'dummy': true }, { safe: true }, (err, docs) => {
                if (err) return reject(err);
                resolve({ latest: docs.ops[0], collection });
              });
            });
      });
    });
  };
}

module.exports = Channel;
