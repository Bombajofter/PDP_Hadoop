
from mrjob.job import MRJob
from mrjob.step import MRStep

class MovieBreakdown (MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings_count_per_movie,
                   reducer=self.reducer_count_ratings ), 
            MRStep(mapper=self.mapper_sort_ratings, reducer=self.reducer_sort_output)
                           ]
    # Door elke review 1 keer te tellen kun je zien wat het aantal reviews is per film
    def mapper_get_ratings_count_per_movie (self, _, line) : 
        (userId, movieId, rating, timestamp) = line.split('\t')
        yield  "Movie: " + movieId, 1  
    
    # Door het aantal reviews bij elkaar op te tellen kan er voor elke film worden bepaald wat het aantal reviews is
    # Het resultaat van deze methode is de uitkomst van de 1e assignment
    def reducer_count_ratings (self, movie, ratingResult):
        yield  movie, sum(ratingResult)
    
    #Om de lijst te sorteren moet er eerst een key value generator gemaakt worden van het voorgaande resultaat
    #Dit resultaat bevat een movieId en het aantal keren dat hiervoor een rating is gegeven
    def mapper_sort_ratings (self, movie, ratingCount):
        yield None, (ratingCount, movie)
    
    #In deze methode wordt de generator via de sorted methode omgezet in een gesorteerd array
    #Hierna kan er over deze array heen geitereerd worden om de resultaten te laten zien
    def reducer_sort_output(self, _, movie):
        movie = sorted(movie)
        for record in movie :
            yield record[1], record[0]

    #Deze methode is nodig om het script vanuit de command line uit te voeren
if __name__ == '__main__':
    MovieBreakdown.run()