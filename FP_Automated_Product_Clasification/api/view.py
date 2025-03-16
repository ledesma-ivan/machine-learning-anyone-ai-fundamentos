import mid

from clear.text_normalizer import normalize
from flask import (
    Blueprint,
    render_template,
    request,
    make_response,
    flash
)
import mysql.connector


router = Blueprint("app_router", __name__, template_folder="templates")



@router.route("/", methods=["GET"])
def index():

    '''
    Index endpoint, renders our HTML code.

    '''

    return render_template("page.html")



@router.route("/", methods=["POST"])
def upload_text():

    '''
    Function used in our frontend so we can upload the text.
    When it receives a text from the UI, it also calls our ML model to get and display the predictions.

    '''

    # Get name of product
    name = request.form.get('inputName')
    
    # Get name of description
    descrpition = request.form.get('inputDescription')
    
    # Create variable text with name + description 
    text = name + ' ' + descrpition
    
    # Clear text
    text = normalize(text)

    # Generate prediction
    context = mid.model_predict(text=text)
    
    # If context is empty
    if context == []:

        return render_template("page.html",context='None',text=text)
    
    # If context is not empty
    else:

        return render_template("page.html",context=context,text=text)


@router.route("/feedback", methods=["GET","POST"])
def feedback():

    '''
    This view stores the text plus the correct categories.

    '''

    if request.form.get("lista") or request.form.get("lista0") or request.form.get("lista1"):
       
        if request.form.get("lista") and request.form.get("lista0") and request.form.get("lista1"):
            
            report =request.form.get("lista") + ',' +request.form.get("lista0") + ','+request.form.get("lista1")
  
        elif not request.form.get("lista") and not request.form.get("lista0"):

            report = request.form.get("lista1")
        
        elif not request.form.get("lista0") and not request.form.get("lista1"):

            report = request.form.get("lista")

        elif not request.form.get("lista") and  not request.form.get("lista1"):

            report = request.form.get("lista0")

        elif not request.form.get("lista"):

            report = request.form.get("lista0") + ','+ request.form.get("lista1")

        elif not request.form.get("lista0"):

            report = request.form.get("lista") + ','+request.form.get("lista1")

        elif not request.form.get("lista1"):

            report = request.form.get("lista") +',' + request.form.get("lista0")
    else:

        return render_template("page.html",load=True)
    
    

    text = request.form.get('report')
    
    db1 = mysql.connector.connect(host='db',user='root',password='ivan',port=3306,database='proyect') 
  
    mydb = db1.cursor()

    query = ("insert into proyect.PROYECT(Text,allCategory) VALUES (%s,%s)")

    var = (str(text),str(report))

    mydb.execute(query,var)
    
    db1.commit()
    
    return render_template("page.html",load=report)



@router.route("/team", methods=["POST"])
def team():
   return render_template('team.html')




@router.route("/predict", methods=["POST"])
def predict():

    '''
    This view is what allows the user to access our API to make predictions.  

    '''

    # Get name of product
    name = request.form.get('inputName')

    # Get description of product 
    description = request.form.get('inputDescription')
   

    # check if name or description is None
    if name is None or description is None:

        # if some is None
        return make_response({"Text": None, "category": None}, 400)

    # If the variables are not empty
    else:

        # Create variable text with name + description 
        text = name + ' ' + description

        # Clear text
        text = normalize(str(text))

        # Generate prediction
        context = mid.model_predict(text=text)

        # If not exist prediction
        if context == []:

            return make_response("Sorry we couldn't detect any category for this text", 200)
        
        # If exist prediction
        else:

            return make_response({"Text": text, "category": context}, 200)