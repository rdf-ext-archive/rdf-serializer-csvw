const Readable = require('readable-stream')

class SerializerStream extends Readable {
  constructor (input, options) {
    super({
      read: () => {}
    })

    this.metadata = options.metadata
    this.buffer = []
    this.wroteHeader = false

    input.on('data', (quad) => {
      this.processQuad(quad)
    })

    input.on('end', () => {
      this.flushBuffer()
      this.push(null)
    })

    input.on('error', (err) => {
      this.emit('error', err)
    })
  }

  processQuad (quad) {
    if (this.processAll) {
      return this.buffer.push(quad)
    }

    if (this.buffer.length && !this.buffer[this.buffer.length - 1].subject.equals(quad.subject)) {
      this.flushBuffer()
    }

    this.buffer.push(quad)
  }

  flushBuffer () {
    const groups = this.groupQuads(this.buffer)

    if (groups.length === 0) {
      return
    }

    if (!this.wroteHeader) {
      this.writeHeader(groups[Object.keys(groups)[0]])
    }

    Object.keys(groups).forEach((subject) => {
      const quads = groups[subject]

      const row = Object.keys(this.columns).reduce((row, name) => {
        const quad = quads.filter((quad) => {
          return quad.predicate.value === this.columns[name]
        }).shift()

        row[name] = (quad && quad.object.value) || ''

        return row
      }, {})

      const subjectName = Object.keys(this.columns).filter((name) => {
        return this.columns[name] === ''
      }).shift()

      if (subjectName) {
        row[subjectName] = subject
      }

      // only add lines which are not empty
      if (Object.keys(row).some(name => row[name])) {
        this.push(this.rowToLine(row) + '\n')
      }
    })
  }

  writeHeader (quads) {
    if (this.metadata) {
      this.columns = this.metadata.tableSchema.columns.reduce((columns, column) => {
        columns[column.name] = column.propertyUrl

        return columns
      }, {})

      if (this.metadata.tableSchema.aboutUrl) {
        this.columns[this.metadata.tableSchema.aboutUrl] = ''
      }
    }

    if (!this.columns) {
      this.columns = quads.reduce((columns, quad) => {
        columns[quad.predicate.value] = quad.predicate.value

        return columns
      }, {})

      this.columns['subject'] = ''
    }

    const headerLine = Object.keys(this.columns).join(',')

    this.push(headerLine + '\n')

    this.wroteHeader = true
  }

  rowToLine (row) {
    return Object.keys(row).map((column) => {
      return row[column]
    }).join(',')
  }

  groupQuads (quads) {
    return quads.reduce((groups, quad) => {
      groups[quad.subject.value] = groups[quad.subject.value] || []
      groups[quad.subject.value].push(quad)

      return groups
    }, {})
  }
}

module.exports = SerializerStream
