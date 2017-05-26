from mrjob.job import MRJob

class MRMaxTemperature(MRJob):

    def MakeFahrenheit(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        fahrenheit = celsius * 1.8 + 32.0
        return fahrenheit


    def MakeCelsius(self, tenthsOfCelsius):
        celsius = float(tenthsOfCelsius) / 10.0
        return celsius

    def mapper(self, _, line):
        (location, date, type, temp, x, y, z, w) = line.split(',')
        if (type == 'TMAX'):
            temperature = self.MakeCelsius(temp)
            yield location, temperature

    def reducer(self, location, temps):
        yield location, max(temps)


if __name__ == '__main__':
    MRMaxTemperature.run()


# example:
# python Max-Temperatures.py data/1800.csv
