const parseEvents = (events) => {
    let stEvent = null
    const parsedEvents = []
    while (stEvent !== -1) {
      const nxtStEvent = events.indexOf('*', stEvent + 1)
      const l = stEvent === null ? 0 : stEvent
      const r = nxtStEvent === -1 ? events.length : nxtStEvent
      parsedEvents.push(events.substring(l, r))
      stEvent = nxtStEvent
    }
  
    return parsedEvents
  }
  
  const isValidCommand = (command, commandLength) => {
    if (!commandLength.startsWith('$')) throw new Error('Incorrect Length')
    const commandLen = Number(commandLength.slice(1))
    if (command.length !== commandLen) throw new Error('Incorrect Element Length')
  }
  
  const parseRequest = (request) => {
    // Check if request starts with '*', throw error if not
    if (request[0] != '*') {
      throw new Error('Invalid Request')
    }
  
    // Split the request into items and remove the last empty one
    const requestItems = request.split(/\r\n/).slice(undefined, -1)
  
    // Get the total number of commands expected
    const totalCommands = Number(requestItems[0].slice(1))
  
    // Separate and count the command lengths and actual commands
    const commandLength = requestItems.slice(1).filter((c) => c.startsWith('$'))
    const commandsList = requestItems.slice(1).filter((c) => !c.startsWith('$'))
  
    // Check if counts match, throw error if not
    if (
      totalCommands !== commandLength.length &&
      totalCommands !== commandsList.length
    ) {
      throw new Error('Invalid Number Of Arguments')
    }
  
    commandsList.forEach((command, idx) =>
      // Validate each command's length, throw error if mismatch
      isValidCommand(command, commandLength[idx])
    )
    return commandsList
  }

  module.exports = {
    parseEvents,
    isValidCommand,
    parseRequest
  }