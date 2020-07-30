import csv
from itertools import chain

from django.http import StreamingHttpResponse


class CSVBuffer:
    """An object that implements just the write method of the file-like
    interface.
    """

    def write(self, value):
        """Return the string to write."""
        return value


class CSVStream:
    """Class to stream (download) an iterator to a 
    CSV file."""

    def export(self, filename, iterator, serializer, header):
        # 1. Create our writer object with the pseudo buffer
        writer = csv.writer(CSVBuffer())

        # 2. Create the StreamingHttpResponse using our iterator as streaming content
        response = StreamingHttpResponse(
            chain(
                (writer.writerow(col for col in header)),
                (writer.writerow(serializer(data)) for data in iterator),
            ),
            content_type="text/csv",
        )

        # 3. Add additional headers to the response
        response['Content-Disposition'] = f"attachment; filename={filename}.csv"
        # 4. Return the response
        return response
