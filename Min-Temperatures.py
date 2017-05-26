from mrjob.job import MRJob

class MRMinTemperature(MRJob):

    def MakeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit


    def MakeCelsius(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        return celsius

    def mapper(self, _, line):
        (location, date, type, temp, x, y, z, w) = line.split(',')
        if (type == 'TMIN'):
            temperature = self.MakeCelsius(temp)
            yield location, temperature

    def reducer(self, location, temps):
        yield location, min(temps)


if __name__ == '__main__':
    MRMinTemperature.run()


# example:
# python Min-Temperatures.py data/1800.csv
