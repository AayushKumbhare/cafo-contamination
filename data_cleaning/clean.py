import fitz
import os
import shutil

def create_output(pdf_path):
    pdf_output = pdf_path[:53]
    suffix = pdf_path[53:]
    output_path = pdf_output + suffix
    return output_path


def clean_pdf(pdf_input, pdf_output):
    try:
        pdf = fitz.open(pdf_input)
        new_pdf = fitz.open()
        for i in range(min(2, len(pdf))):
            new_pdf.insert_pdf(pdf, from_page=i, to_page=i)

        new_len = len(new_pdf)
        new_pdf.save(pdf_output)
        new_pdf.close()
        pdf.close()

        print(f"Extracted: {new_len} pages from {pdf_input}")
        return True

    except Exception as e:
        print(f"Error: {e}")
        return False

if __name__ == "__main__":
    directory_path = '/Users/aayushkumbhare/Desktop/cafo-contamination/2022'
    destination_path = '/Users/aayushkumbhare/Desktop/cafo-contamination/cleaned_2022'
    with os.scandir(directory_path) as entries:
        for i, entry in enumerate(entries):
            if entry.is_file():
                pdf_path = entry.path
                output_path = create_output(pdf_path)
                clean_pdf(pdf_path, output_path)

            if not os.path.exists(destination_path):
                os.makedirs(destination_path, exist_ok=True) 
            try:
                shutil.move(entry, destination_path)
                print(f"Moved '{entry}' to '{destination_path}'")
            except FileNotFoundError:
                print("Source file or destination directory not found.")
            except Exception as e:
                print(f"An error occurred: {e}")
            print(f"Cleaned and put file number {i} in output folder")

