import re
import nltk
import spacy
import unicodedata

from bs4 import BeautifulSoup
from contractions import CONTRACTION_MAP
from nltk.tokenize.toktok import ToktokTokenizer

from tqdm import tqdm

nltk.download('stopwords')

tokenizer = ToktokTokenizer()
stemmer = nltk.stem.PorterStemmer()
stopword_list = nltk.corpus.stopwords.words('english')
nlp = spacy.load('en_core_web_sm')


def remove_html_tags(text):
    # Put your code
    text = BeautifulSoup(text, "html.parser").get_text()
    return text


def stem_text(text):
    # Put your code
    text_token = tokenizer.tokenize(text)
    text_list = [stemmer.stem(word) for word in text_token]
    text = " ".join(map(str,text_list))
    return text


def lemmatize_text(text):
    # Put your code
    doc = nlp(text)
    words_lemmas_list = [token.lemma_ for token in doc]
    text = " ".join(map(str,words_lemmas_list))
    return text


def expand_contractions(text, contraction_mapping=CONTRACTION_MAP):
    # Put your code
    contractions_re = re.compile('(%s)' % '|'.join(contraction_mapping.keys())) #Compile the list of contractions
    def replace(match):
        return contraction_mapping[match.group(0)] #Returns expanded word for a contraction
    return contractions_re.sub(replace, text) #Replace matching words with contractions_re from text


def remove_accented_chars(text):
    # Put your code
    nfkd_form = unicodedata.normalize('NFKD', text)
    text = "".join([c for c in nfkd_form if not unicodedata.combining(c)])#Returns only letters
    return text


def remove_special_chars(text, remove_digits=False):
    # Put your code
    if remove_digits:
        text = re.sub("[^a-zA-Z\s]", "", text) #Remove non-letters
    else:
        text = re.sub("[^a-zA-Z0-9\s]", "", text) #Remove non-letters and non-numbers
    return text


def remove_stopwords(text, is_lower_case=False, stopwords=stopword_list):
    # Put your code
    text_tokens = tokenizer.tokenize(text)
    if is_lower_case:
        token_text = [word for word in text_tokens if not word in stopwords]
    else:
        token_text = [word for word in text_tokens if not word.lower() in stopwords]
    text = ' '.join(token_text)
    return text


def remove_extra_new_lines(text):
    # Put your code
    text = re.sub(r'\n', ' ', text)
    return text


def remove_extra_whitespace(text):
    # Put your code
    text = " ".join(text.split())
    return text
    

def normalize_corpus(
    corpus,
    html_stripping=True,
    contraction_expansion=True,
    accented_char_removal=True,
    text_lower_case=True,
    text_stemming=False,
    text_lemmatization=False,
    special_char_removal=True,
    remove_digits=True,
    stopword_removal=True,
    stopwords=stopword_list
):
    
    normalized_corpus = []

    # Normalize each doc in the corpus
    for doc in tqdm(corpus):
        # Remove HTML
        if html_stripping:
            doc = remove_html_tags(doc)
            
        # Remove extra newlines
        doc = remove_extra_new_lines(doc)
        
        # Remove accented chars
        if accented_char_removal:
            doc = remove_accented_chars(doc)
            
        # Expand contractions    
        if contraction_expansion:
            doc = expand_contractions(doc)
            
        # Lemmatize text
        if text_lemmatization:
            doc = lemmatize_text(doc)
            
        # Stemming text
        if text_stemming and not text_lemmatization:
            doc = stem_text(doc)
            
        # Remove special chars and\or digits    
        if special_char_removal:
            doc = remove_special_chars(
                doc,
                remove_digits=remove_digits
            )  

        # Remove extra whitespace
        doc = remove_extra_whitespace(doc)

         # Lowercase the text    
        if text_lower_case:
            doc = doc.lower()

        # Remove stopwords
        if stopword_removal:
            doc = remove_stopwords(
                doc,
                is_lower_case=text_lower_case,
                stopwords=stopwords
            )

        # Remove extra whitespace
        doc = remove_extra_whitespace(doc)
        doc = doc.strip()
            
        normalized_corpus.append(doc)
        
    return normalized_corpus
