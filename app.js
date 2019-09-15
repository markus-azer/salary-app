const csv = require('csv-parser');
const fs = require('fs');
const Readable = require('stream').Readable;
const Json2csvTransform = require('json2csv').Transform;

const emptyObject = obj => Object.entries(obj).length === 0 && obj.constructor === Object;

const getNext12MonthsHeader = (month, year) => {
  const months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

  let result = [];
  for (var i = 0; i < 13; ++i) {
    result.push(`${months[month]} ${year} Salary`);
    result.push(`${months[month]} ${year} Salary Date`);
    result.push(`${months[month]} ${year} Bonus`);
    result.push(`${months[month]} ${year} Bonus Date`);

    if (++month === 12) {
      month = 0;
      ++year;
    };
  };

  return result;
};

const getNext12Months = (month, year) => {
  const months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];

  let result = [];
  for (var i = 0; i < 13; ++i) {
    result.push(`${months[month]} ${year}`);

    if (++month === 12) {
      month = 0;
      ++year;
    };
  };

  return result;
};

const createCsvHeaders = (month, year) => {
  const next12MonthsHeader = getNext12MonthsHeader(month, year);
  return ['Name', ...next12MonthsHeader];
}

const lastBusinessDayOfMonth = (monthName, year) => {
  const months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
  const month = months.findIndex(month => month == monthName) + 1;
  let offset = 0;
  let result = null;

  do {
    result = new Date(year, month, offset);
    offset--;
  } while (5 === result.getDay() || 6 === result.getDay());

  return result.toDateString();
};

const bonusDayOfMonth = (monthName, year) => {
  const months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
  let month = months.findIndex(month => month == monthName) + 1;
  let offset = 15;
  let result = new Date(year, month, offset);

  if(5 === result.getDay() || 6 === result.getDay()){
    do {
      result = new Date(year, month, offset);
      offset++;
    } while (0 === result.getDay() || 1 === result.getDay() || 2 === result.getDay() || 6 === result.getDay() || 5 === result.getDay());
  }

  return result.toDateString();
};

const transformRow = (row, month, year, savedDates) => {
  const next12Months = getNext12Months(month, year);
  return Object.entries(row).reduce((acc, [key, value], i)=>{
    if(i == 0){
      acc[key] = value;
    }
    //Determine if its Salary or Bonus
    if(/Salary/i.test(key)){
      const monthYear = key.replace(/ Salary/i, '');
      if(next12Months.findIndex(month => month.toLocaleLowerCase() == monthYear.toLocaleLowerCase()) !== -1 ){
        const splittedYearMonth = monthYear.split(' ');
        const lastWorkingDay = lastBusinessDayOfMonth(splittedYearMonth[0], splittedYearMonth[1]);
        acc[key] = value;
        acc[`${key} Date`] = savedDates[`${key} Date`] || lastWorkingDay;
      }
    }else if(/Bonus/i.test(key)){
      const monthYear = key.replace(/ Bonus/i, '');
      if(next12Months.findIndex(month => month.toLocaleLowerCase() == monthYear.toLocaleLowerCase()) !== -1 ){
        const splittedYearMonth = monthYear.split(' ');
        const bonusDay = bonusDayOfMonth(splittedYearMonth[0], splittedYearMonth[1]);
        acc[key] = value;
        acc[`${key} Date`] = savedDates[`${key} Date`] || bonusDay;
      }
    };

    return acc;
  },{});
};

const main = () => {
  try{
    const input = new Readable({ objectMode: true, read() {} });
    const inputTwo = new Readable({ objectMode: true, read() {} });
    const writeFileStream = fs.createWriteStream('Salary Dates.csv', { flags: 'w' });
  
    const currentDate = new Date();
    const month = currentDate.getMonth();
    const year = currentDate.getFullYear();
  
    const fields = createCsvHeaders(month, year);
    const opts = { fields };
    let savedDates = {}
    //Read csv as Stream
    fs.createReadStream('./Salary.csv')
      .on('error', (error) => {
        console.log(`ERROR ===> ${error}`);
      })
      .pipe(csv())
      .on('data', (row) => {
        const transformedRow = transformRow(row, month, year, savedDates);
        if (emptyObject(savedDates)) savedDates = transformedRow;
        input.push(transformedRow);
      })
      .on('end', () => {
        console.log('Data loaded');
        input.push(null);
      })
      .on('error', (error) => {
        console.log(`ERROR ===> ${error}`);
      });
  
    // Transform Data to Csv
    const transform = new Json2csvTransform(opts, { objectMode: true });
    const processor = input.pipe(transform);
  
    // Pipe data to WritableStream
    processor
      .on('data', (chunk) => {
        inputTwo.push(chunk);
      })
      .on('end', () => inputTwo.push(null))
      .on('error', err => console.log(`ERROR ===> ${err.message}`));
  
    inputTwo.pipe(writeFileStream);
    writeFileStream.on('close', () => {
      console.log('All done!');
    });
  }catch(err){
    console.log(`ERROR ===> ${err}`);
  }
}

//excute the main
main();