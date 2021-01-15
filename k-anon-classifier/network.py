import os
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import tensorflow as tf
import keras_preprocessing
from keras_preprocessing import image
from keras_preprocessing.image import ImageDataGenerator
import numpy as np
import json

def train():
    incognito_training_dir = os.path.join('train/incognito')
    datafly_training_dir = os.path.join('train/datafly')

    print('total training incognito images:', len(os.listdir(incognito_training_dir)))
    print('total training datafly images:', len(os.listdir(datafly_training_dir)))

    TRAINING_DIR = "train/"
    VALIDATION_DIR = "val/"

    training_datagen = ImageDataGenerator(
        rescale=1. / 255,
        rotation_range=40,
        width_shift_range=0.2,
        height_shift_range=0.2,
        shear_range=0.2,
        zoom_range=0.2,
        horizontal_flip=True,
        fill_mode='nearest')

    validation_datagen = ImageDataGenerator(rescale=1. / 255)

    train_generator = training_datagen.flow_from_directory(
        TRAINING_DIR,
        target_size=(600, 600),
        class_mode='categorical',
        batch_size=2
    )

    label_map = (train_generator.class_indices)

    validation_generator = validation_datagen.flow_from_directory(
        VALIDATION_DIR,
        target_size=(600, 600),
        class_mode='categorical',
        batch_size=2
    )

    model = tf.keras.models.Sequential([
        # Note the input shape is the desired size of the image 150x150 with 3 bytes color
        # This is the first convolution
        tf.keras.layers.Conv2D(64, (3, 3), activation='relu', input_shape=(600, 600, 3)),
        tf.keras.layers.MaxPooling2D(2, 2),
        # The second convolution
        tf.keras.layers.Conv2D(64, (3, 3), activation='relu'),
        tf.keras.layers.MaxPooling2D(2, 2),
        # The third convolution
        tf.keras.layers.Conv2D(128, (3, 3), activation='relu'),
        tf.keras.layers.MaxPooling2D(2, 2),
        # The fourth convolution
        tf.keras.layers.Conv2D(128, (3, 3), activation='relu'),
        tf.keras.layers.MaxPooling2D(2, 2),
        # Flatten the results to feed into a DNN
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dropout(0.5),
        # 512 neuron hidden layer
        tf.keras.layers.Dense(512, activation='relu'),
        tf.keras.layers.Dense(2, activation='softmax')
    ])

    model.summary()

    model.compile(loss='categorical_crossentropy', optimizer='rmsprop', metrics=['accuracy'])

    history = model.fit(train_generator, epochs=25, steps_per_epoch=20, validation_data=validation_generator, verbose=1,
                        validation_steps=3)

    model.save("rps.h5")
    with open('labels.json', 'w') as f:
        json.dump(label_map, f)

    acc = history.history['accuracy']
    val_acc = history.history['val_accuracy']
    loss = history.history['loss']
    val_loss = history.history['val_loss']

    epochs = range(len(acc))

    plt.plot(epochs, acc, 'r', label='Training accuracy')
    plt.plot(epochs, val_acc, 'b', label='Validation accuracy')
    plt.title('Training and validation accuracy')
    plt.legend(loc=0)
    plt.figure()

    plt.show()


def use():
    model = tf.keras.models.Sequential([
        # Note the input shape is the desired size of the image 150x150 with 3 bytes color
        # This is the first convolution
        tf.keras.layers.Conv2D(64, (3, 3), activation='relu', input_shape=(600, 600, 3)),
        tf.keras.layers.MaxPooling2D(2, 2),
        # The second convolution
        tf.keras.layers.Conv2D(64, (3, 3), activation='relu'),
        tf.keras.layers.MaxPooling2D(2, 2),
        # The third convolution
        tf.keras.layers.Conv2D(128, (3, 3), activation='relu'),
        tf.keras.layers.MaxPooling2D(2, 2),
        # The fourth convolution
        tf.keras.layers.Conv2D(128, (3, 3), activation='relu'),
        tf.keras.layers.MaxPooling2D(2, 2),
        # Flatten the results to feed into a DNN
        tf.keras.layers.Flatten(),
        tf.keras.layers.Dropout(0.5),
        # 512 neuron hidden layer
        tf.keras.layers.Dense(512, activation='relu'),
        tf.keras.layers.Dense(2, activation='softmax')
    ])

    model.load_weights("rps.h5")
    with open('labels.json', 'r') as file:
        labels = json.load(file)



    pathDatafly = 'predict/datafly'
    pathIncognito = 'predict/incognito'
    for i in range(98, 123):
        img = image.load_img(pathDatafly + '.' + str(i) + '.jpg', target_size=(600, 600))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        images = np.vstack([x])
        predictions = model.predict(images, batch_size=2)
        val = np.argmax(predictions[0])
        for key in labels:
            if labels[key] == val:
                #print('datafly', key)
                if key == 'datafly':
                    print('true')
                else:
                    print('false')

    for i in range(98, 123):
        img = image.load_img(pathIncognito + '.' + str(i) + '.jpg', target_size=(600, 600))
        x = image.img_to_array(img)
        x = np.expand_dims(x, axis=0)
        images = np.vstack([x])
        predictions = model.predict(images, batch_size=2)
        val = np.argmax(predictions[0])
        for key in labels:
            if labels[key] == val:
                #print('incognito', key)
                if key == 'incognito':
                    print('true')
                else:
                    print('false')



def main():
    #train()
    use()


if __name__ == '__main__':
    main()