
# coding: utf-8

# In[4]:

from flask import Blueprint
main = Blueprint('main', __name__)
from flask import Flask, render_template, request, redirect, url_for, send_from_directory
from werkzeug import secure_filename
import json
from engine import RecommendationEngine
import config

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from flask import Flask, request
from flask import render_template
from flask import flash, redirect, url_for

from flask_wtf import Form
from wtforms import StringField, BooleanField,SelectField
from wtforms.validators import DataRequired
app = Flask(__name__)

import os 

    
class TopRatings(Form):
    user_id = StringField('user_id', validators=[DataRequired()])
    count = StringField('count',validators=[DataRequired()])
    
    
class BusinessRatings(Form):
    user_id = StringField('user_id', validators=[DataRequired()])
    business_id = StringField('business_id',validators=[DataRequired()])    

class AddRatings(Form):
    user_id = StringField('user_id', validators=[DataRequired()])
    
# In[5]:
@main.route("/")
def home():
    user = {'nickname': 'Anoop'}  # fake user
    return render_template('home.html',
                           title='Yelp Recommender',
                           user=user)

@main.route("/Popularity_recommender_form",methods= ['GET','POST'])
def Popularity_recommender_form():
    user = {'nickname':'Anoop'}  
    return render_template('Popularity_recommender_form.html',
                           title='Personalised Recommender',
                           user = user)

@main.route("/Popularity_recommender_engine", methods= ['GET','POST'])
def Popularity_recommender_engine():       
    user = {'nickname': 'Anoop'}  # fake user
    if request.method == 'POST':
        location = request.form.get('location')  
    top_ratings = recommendation_engine.popularity_model(location)
    return render_template('Popularity_recommender_engine.html',
                           title='Yelp Recommender',
                           ratings = top_ratings,
                           user=user)

@main.route("/Personalised_recommender_form",methods= ['GET','POST'])
def Personalised_recommender_form():
    user = {'nickname':'Anoop'}  
    form = TopRatings()
    if form.validate_on_submit():
        user_id = form.user_id.data
        count = form.count.data
        return redirect(url_for('main.Top_ratings_engine',user_id =user_id,count = count))
    return render_template('Personalised_recommender_form.html',
                           title='Personalised Recommender',
                           user = user,
                           form = form)

@main.route("/<int:user_id>/ratings/<int:count>", methods=["GET"])
def Top_ratings_engine(user_id, count):
    logger.debug("User %s TOP ratings requested", user_id)
    top_ratings = recommendation_engine.get_top_ratings(user_id,count)
    user = {'nickname': 'Anoop'}
    user_id = user_id
    return render_template('Ratings.html',
                            title = 'Yelp Recommender',
                            user = user,
                            user_id = user_id,
                            ratings = top_ratings)

@main.route("/Business_ratings_form",methods= ['GET','POST'])
def Business_ratings_form():
    user = {'nickname':'Anoop'}  
    form = BusinessRatings()
    if form.validate_on_submit():
        user_id = form.user_id.data
        business_id = form.business_id.data
        return redirect(url_for('main.Business_ratings_engine',user_id =user_id,business_id = business_id))
    return render_template('Business_ratings_form.html',
                           title='Personalised Business Ratings',
                           user = user,
                           form = form)

@main.route("/<int:user_id>/business_ratings/<int:business_id>", methods=["GET"])
def Business_ratings_engine(user_id, business_id):
    logger.debug("User %s rating requested for business %s", user_id, business_id)
    top_ratings = recommendation_engine.get_ratings_for_business_ids(user_id, [business_id])
    user = {'nickname': 'Anoop'}
    user_id = user_id
    return render_template('Ratings.html',
                           title = 'Yelp Recommender',
                           user = user,
                           user_id = user_id,
                           ratings = top_ratings)

@main.route('/add_ratings_form', methods=['POST','GET'])
def add_ratings_form():
    user = {'nickname':'Anoop'}  
    form = AddRatings()
    if form.validate_on_submit():
        user_id = form.user_id.data
        return redirect(url_for('main.add_ratings_engine',user_id = user_id))
    return render_template('add_ratings_form.html', title='Upload Ratings',
                           user = user,form = form )

@main.route('/add_ratings_upload', methods=['POST','GET'])
def add_ratings_upload():
    # Get the name of the uploaded file
    if request.method == 'POST':
        # check if the post request has the file part
        
        file = request.files['file']
        # if user does not select file, browser also
        # submit a empty part without filename
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file.save(os.path.join('uploads/', filename))
            return redirect(url_for('main.uploaded_file',
                                    filename=filename))
        return redirect(url_for('main.add_ratings_form'))
    user = {'nickname':'Anoop'}     
    return render_template('upload.html', title='Upload Ratings',
                           user = user )


@main.route("/<int:user_id>/ratings", methods = ["GET"])
def add_ratings_engine(user_id):
    # get the ratings from the Flask POST request object
    f = open('uploads/ratings.file', 'r')
    a= f.read()
    ratings_list = a.split("\n")
    ratings_list = map(lambda x: x.split(","), ratings_list[0:len(ratings_list)-1])
    # create a list with the format required by the negine (user_id, movie_id, rating)
    ratings = map(lambda x: (user_id, int(x[0]), float(x[1])), ratings_list)
    # add them to the model using then engine API
    recommendation_engine.add_ratings(ratings)
    top_ratings = recommendation_engine.get_top_ratings(user_id,10)
    user = {'nickname': 'Anoop'}
    user_id = user_id
    return render_template('Ratings.html',
                            title = 'Yelp Recommender',
                            user = user,
                            user_id = user_id,
                            ratings = top_ratings)



def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in set(['txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif','file'])


@main.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory('uploads/',
                               filename)



def create_app(spark_context):
    global recommendation_engine 

    recommendation_engine = RecommendationEngine(spark_context)    

    app = Flask(__name__)
    
    app.register_blueprint(main)

    return app
